/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.server.grpc;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.ProtocolContext;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.MutableEmbeddedDocument;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.GraphBatch;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.executor.QueryStatistics;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.VertexType;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.security.SecurityManager;
import com.arcadedb.serializer.JsonSerializer;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.grpc.InsertOptions.ConflictMode;
import com.arcadedb.server.grpc.InsertOptions.TransactionMode;
import com.arcadedb.server.grpc.ProjectionSettings.ProjectionEncoding;
import com.arcadedb.server.monitor.QueryProfile;
import com.arcadedb.server.monitor.ServerQueryProfiler;
import com.arcadedb.server.security.ServerSecurity;
import com.arcadedb.server.security.ServerSecurityException;
import com.arcadedb.server.security.ServerSecurityUser;
import io.micrometer.core.instrument.Metrics;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.jspecify.annotations.NonNull;

import com.arcadedb.utility.DateUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.logging.Level;

/**
 * gRPC Service implementation for ArcadeDB
 */
public class ArcadeDbGrpcService extends ArcadeDbServiceGrpc.ArcadeDbServiceImplBase {

  // Pick serializer once
  private static final JsonSerializer FAST = JsonSerializer.createJsonSerializer().setIncludeVertexEdges(false)
      .setUseVertexEdgeSize(true)
      .setUseCollectionSizeForEdges(true).setUseCollectionSize(false);

  private static final JsonSerializer SAFE = JsonSerializer.createJsonSerializer().setIncludeVertexEdges(false)
      .setUseVertexEdgeSize(true)
      .setUseCollectionSizeForEdges(false).setUseCollectionSize(false);

  private static final int GRAPH_BATCH_VERTEX_BUFFER = 10_000;

  // Transaction management - now stores TransactionContext with executor for thread affinity
  private final Map<String, TransactionContext> activeTransactions = new ConcurrentHashMap<>();

  /**
   * Holds transaction state including a single-thread executor to ensure all
   * transaction operations run on the same thread (required by ArcadeDB's thread-local transactions).
   */
  private static final class TransactionContext {
    final Database        db;
    final ExecutorService executor;
    final String          txId;
    // Principal that opened this transaction. Bound at beginTransaction so a transaction-scoped RPC can
    // reject any caller other than the owner. Null only when the transaction was opened on a server with
    // security disabled (open mode), where there is no authenticated identity to bind to.
    final String          owner;
    final long            createdAtMs;
    volatile long         lastAccessMs;

    TransactionContext(Database db, String txId, String owner) {
      this.db = db;
      this.txId = txId;
      this.owner = owner;
      this.createdAtMs = System.currentTimeMillis();
      this.lastAccessMs = this.createdAtMs;
      // Single-thread executor ensures all tx operations happen on the same thread
      this.executor = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "arcadedb-tx-" + txId);
        t.setDaemon(true);
        return t;
      });
    }

    /**
     * Records that the transaction has just been used so the idle reaper does not reclaim it while a client is
     * actively working with it.
     */
    void touch() {
      this.lastAccessMs = System.currentTimeMillis();
    }

    void shutdown() {
      executor.shutdown();
    }
  }

  // Database connection pool
  private final Map<String, Database> databasePool = new ConcurrentHashMap<>();

  // Track highest acknowledged chunk per session for idempotency
  private final Map<String, Long> sessionWatermark = new ConcurrentHashMap<>();

  // ArcadeDB server reference (optional, for accessing existing databases)
  private final ArcadeDBServer arcadeServer;

  // Database directory path
  private final String databasePath;

  private static final int DEFAULT_MAX_COMMAND_ROWS = 1000;

  // Idle reaper defaults (milliseconds). A registered transaction idle longer than maxIdle, or older than maxAge
  // (when > 0), is rolled back and released so an abandoned client cannot leak its executor thread, open
  // transaction and database reference forever.
  static final long DEFAULT_TX_MAX_IDLE_MS      = 300_000L; // 5 minutes
  static final long DEFAULT_TX_MAX_AGE_MS       = 0L;       // disabled by default
  static final long DEFAULT_TX_REAPER_PERIOD_MS = 30_000L;  // 30 seconds

  // Concurrent-transaction caps (issue #5048). Each open transaction owns a dedicated executor thread, so an
  // unbounded number of concurrent transactions is a thread/memory-exhaustion DoS vector for an authenticated
  // client. A non-positive value disables the corresponding bound.
  static final int DEFAULT_MAX_CONCURRENT_TX               = 1_000;
  static final int DEFAULT_MAX_CONCURRENT_TX_PER_PRINCIPAL = 100;

  // Sentinel used as the per-principal counter key when a transaction is opened on a security-disabled server
  // (no authenticated identity to bind to).
  private static final String ANONYMOUS_PRINCIPAL = "";

  private final long                     txMaxIdleMs;
  private final long                     txMaxAgeMs;
  private final ScheduledExecutorService txReaper;

  private final int                            maxConcurrentTransactions;
  private final int                            maxConcurrentTransactionsPerPrincipal;
  private final AtomicInteger                  globalTransactionCount     = new AtomicInteger();
  private final Map<String, AtomicInteger>     perPrincipalTransactionCount = new ConcurrentHashMap<>();

  public ArcadeDbGrpcService(String databasePath, ArcadeDBServer server) {
    this(databasePath, server, DEFAULT_TX_MAX_IDLE_MS, DEFAULT_TX_MAX_AGE_MS, DEFAULT_TX_REAPER_PERIOD_MS);
  }

  public ArcadeDbGrpcService(String databasePath, ArcadeDBServer server, final long txMaxIdleMs, final long txMaxAgeMs,
      final long txReaperPeriodMs) {
    this(databasePath, server, txMaxIdleMs, txMaxAgeMs, txReaperPeriodMs, DEFAULT_MAX_CONCURRENT_TX,
        DEFAULT_MAX_CONCURRENT_TX_PER_PRINCIPAL);
  }

  public ArcadeDbGrpcService(String databasePath, ArcadeDBServer server, final long txMaxIdleMs, final long txMaxAgeMs,
      final long txReaperPeriodMs, final int maxConcurrentTransactions, final int maxConcurrentTransactionsPerPrincipal) {
    this.databasePath = databasePath;
    this.arcadeServer = server;
    this.txMaxIdleMs = txMaxIdleMs;
    this.txMaxAgeMs = txMaxAgeMs;
    this.maxConcurrentTransactions = maxConcurrentTransactions;
    this.maxConcurrentTransactionsPerPrincipal = maxConcurrentTransactionsPerPrincipal;

    // Start the reaper only when at least one expiry bound is active and a positive period is configured.
    // A non-positive maxIdleMs/maxAgeMs disables that individual bound; non-positive for both (or a non-positive
    // period) disables the reaper entirely.
    if ((txMaxIdleMs > 0 || txMaxAgeMs > 0) && txReaperPeriodMs > 0) {
      this.txReaper = Executors.newSingleThreadScheduledExecutor(r -> {
        final Thread t = new Thread(r, "arcadedb-grpc-tx-reaper");
        t.setDaemon(true);
        return t;
      });
      this.txReaper.scheduleWithFixedDelay(this::reapIdleTransactions, txReaperPeriodMs, txReaperPeriodMs,
          TimeUnit.MILLISECONDS);
    } else {
      this.txReaper = null;
    }
  }

  /**
   * Returns the active transaction count, exposed for testing and monitoring.
   */
  int getActiveTransactionCount() {
    return activeTransactions.size();
  }

  /**
   * Returns whether the idle-transaction reaper is running. Exposed for testing the configuration wiring.
   */
  boolean isIdleReaperActive() {
    return txReaper != null;
  }

  /**
   * Returns the number of open transactions currently attributed to the given principal. Exposed for testing the
   * per-principal concurrency cap.
   */
  int getTransactionCountForPrincipal(final String owner) {
    final AtomicInteger counter = perPrincipalTransactionCount.get(owner == null ? ANONYMOUS_PRINCIPAL : owner);
    return counter == null ? 0 : counter.get();
  }

  /**
   * Atomically reserves a slot for a new transaction against the global and per-principal caps. Returns {@code true}
   * when both bounds admit the transaction; on rejection no counter is left incremented. A non-positive cap disables
   * that bound. Reserving before the dedicated executor thread is created is what makes the cap effective against a
   * beginTransaction flood: the thread is never allocated for a rejected request.
   */
  boolean tryReserveTransactionSlot(final String owner) {
    final int global = globalTransactionCount.incrementAndGet();
    if (maxConcurrentTransactions > 0 && global > maxConcurrentTransactions) {
      globalTransactionCount.decrementAndGet();
      return false;
    }

    // Reserve the per-principal slot under an atomic compute so admission and the counter mutation happen as one
    // step per key; this also lets releaseTransactionSlot remove the entry at zero without racing a concurrent
    // reserve, keeping perPrincipalTransactionCount from growing unbounded across many distinct principals.
    // The per-principal cap does not apply to the anonymous principal (security-disabled server): a single shared
    // identity is not a fairness boundary, so only the global cap bounds it. The count is still tracked for metrics.
    final String key = owner == null ? ANONYMOUS_PRINCIPAL : owner;
    final int perPrincipalCap = owner == null ? 0 : maxConcurrentTransactionsPerPrincipal;
    final boolean[] admitted = { true };
    perPrincipalTransactionCount.compute(key, (k, counter) -> {
      if (counter == null)
        return new AtomicInteger(1);
      if (perPrincipalCap > 0 && counter.get() >= perPrincipalCap) {
        admitted[0] = false;
        return counter;
      }
      counter.incrementAndGet();
      return counter;
    });

    if (!admitted[0]) {
      globalTransactionCount.decrementAndGet();
      return false;
    }
    return true;
  }

  /**
   * Releases a previously reserved transaction slot. Called exactly once per successful reservation when the
   * transaction is torn down (commit, rollback, reap, or a failed begin) so the caps track live transactions. The
   * per-principal entry is dropped when its count returns to zero so the map does not accumulate stale keys.
   */
  void releaseTransactionSlot(final String owner) {
    globalTransactionCount.decrementAndGet();
    final String key = owner == null ? ANONYMOUS_PRINCIPAL : owner;
    perPrincipalTransactionCount.computeIfPresent(key, (k, counter) -> counter.decrementAndGet() <= 0 ? null : counter);
  }

  /**
   * Looks up an active transaction and refreshes its last-access timestamp so that genuine activity keeps it alive.
   * Returns {@code null} for a blank/unknown id, preserving the previous inline behaviour.
   */
  private TransactionContext lookupActiveTransaction(final String txId) {
    if (txId == null || txId.isBlank())
      return null;
    final TransactionContext ctx = activeTransactions.get(txId);
    if (ctx != null)
      ctx.touch();
    return ctx;
  }

  /**
   * Resolves an active transaction for a transaction-scoped RPC and enforces that the caller is allowed to
   * drive it. Returns {@code null} when no transaction id was supplied (or none is registered) so the
   * caller falls back to the non-transactional path. When a transaction IS found the caller must pass the
   * same authentication + per-database authorization enforced by {@link #getDatabase} and be the principal
   * that opened it; otherwise a gRPC {@code PERMISSION_DENIED} (or {@code UNAUTHENTICATED}) is thrown. This
   * closes the transaction-hijack where a caller guessing another user's transaction id could drive it on a
   * database it cannot otherwise access.
   */
  private TransactionContext resolveAuthorizedTransaction(final String txId, final DatabaseCredentials credentials) {
    final TransactionContext txCtx = lookupActiveTransaction(txId);
    if (txCtx == null)
      return null;
    authorizeTransactionAccess(txCtx, credentials);
    return txCtx;
  }

  /**
   * Authenticates the caller and authorizes them for the transaction's REAL database (never a
   * request-supplied database name), mirroring {@link #getDatabase}'s gate, then enforces transaction
   * ownership: only the principal that opened the transaction may drive it. Throws a gRPC status exception
   * (fail-closed) on any mismatch.
   */
  private void authorizeTransactionAccess(final TransactionContext txCtx, final DatabaseCredentials credentials) {
    // Authorize against the transaction's actual database so a caller cannot lie about the database name.
    validateCredentials(credentials, txCtx.db.getName());

    final String caller = resolvedUsername(credentials);
    if (txCtx.owner != null && !txCtx.owner.equals(caller))
      throw Status.PERMISSION_DENIED.withDescription("Transaction is owned by another user").asRuntimeException();
  }

  /**
   * True when the caller explicitly supplied a non-blank transaction id that resolves to no active
   * transaction - typically because the idle reaper already rolled it back. This is distinct from a
   * genuinely absent/blank id, which legitimately means "no external transaction, use the auto-transaction
   * path". A transaction-scoped write RPC must fail loudly in this case instead of silently auto-committing.
   */
  private static boolean isUnknownSuppliedTransaction(final String txId, final TransactionContext txCtx) {
    return txCtx == null && txId != null && !txId.isBlank();
  }

  /**
   * gRPC status for a write/stream RPC that carried a non-blank transaction id no longer registered on the
   * server. FAILED_PRECONDITION signals the client that the transaction it believed it was inside is gone,
   * so the call must not silently fall through to a per-call auto-commit and lose the atomicity the client
   * expected.
   */
  private static Status unknownTransactionStatus(final String txId) {
    return Status.FAILED_PRECONDITION.withDescription(
        "Unknown or expired transaction id: " + txId + " (it may have been rolled back by the idle reaper)");
  }

  /**
   * Scans the registered transactions and reclaims any that have been idle past {@code txMaxIdleMs}, or (when
   * configured) older than {@code txMaxAgeMs}. Each reaped transaction is removed atomically so the sweep never
   * races a concurrent commit/rollback, then rolled back on its own dedicated thread and its executor shut down.
   */
  private void reapIdleTransactions() {
    try {
      final long now = System.currentTimeMillis();
      int reaped = 0;
      for (final Map.Entry<String, TransactionContext> entry : activeTransactions.entrySet()) {
        final TransactionContext ctx = entry.getValue();
        final long idleMs = now - ctx.lastAccessMs;
        final long ageMs = now - ctx.createdAtMs;
        final boolean idleExpired = txMaxIdleMs > 0 && idleMs >= txMaxIdleMs;
        final boolean ageExpired = txMaxAgeMs > 0 && ageMs >= txMaxAgeMs;
        if (!idleExpired && !ageExpired)
          continue;

        // CON-5: for an idle-only reap, re-read lastAccessMs immediately before claiming the transaction so a
        // request that touched it (via lookupActiveTransaction()) between the staleness read above and the
        // remove below is not reaped out from under an active caller. An age-expired transaction is reaped
        // regardless, since createdAtMs never advances on touch().
        if (!ageExpired) {
          final long freshIdleMs = System.currentTimeMillis() - ctx.lastAccessMs;
          if (txMaxIdleMs <= 0 || freshIdleMs < txMaxIdleMs)
            continue;
        }

        // remove(key, value) ensures only one of the reaper / commit / rollback wins the cleanup.
        // Residual TOCTOU note: a request could lookupActiveTransaction() (touch + submit work) in the instant
        // between the re-read above and this remove. That window only opens for an already-idle
        // transaction; any command already submitted to the executor still runs to completion before the
        // asynchronous shutdown() in reapTransaction() lets the thread terminate, so no in-flight work is lost.
        if (activeTransactions.remove(entry.getKey(), ctx)) {
          LogManager.instance().log(this, Level.FINE,
              "Reaping abandoned gRPC transaction txId=%s (idleMs=%s ageMs=%s)", entry.getKey(), idleMs, ageMs);
          releaseTransactionSlot(ctx.owner);
          reapTransaction(ctx);
          reaped++;
        }
      }
      // A single summary line per sweep instead of one WARNING per transaction avoids flooding the log when a
      // burst of abandoned transactions is reclaimed at once.
      if (reaped > 0)
        LogManager.instance().log(this, Level.WARNING,
            "Reaped %s abandoned gRPC transaction(s): rolled back and released their executor/database resources",
            reaped);
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING, "Error while reaping idle gRPC transactions", e);
    }
  }

  /**
   * Rolls back the abandoned transaction on its dedicated thread (where its DatabaseContext lives) and shuts the
   * executor down. The rollback is submitted without blocking the single reaper thread: {@code shutdown()} (not
   * {@code shutdownNow()}) lets the just-submitted rollback run to completion before the executor terminates, so a
   * slow rollback never stalls reclamation of the remaining transactions in the same sweep.
   */
  private void reapTransaction(final TransactionContext txCtx) {
    try {
      if (txCtx.db != null && txCtx.db.isOpen()) {
        txCtx.executor.submit(() -> {
          try {
            txCtx.db.rollback();
          } catch (final Exception e) {
            LogManager.instance().log(this, Level.WARNING, "Failed to rollback abandoned transaction %s during reaping",
                e, txCtx.txId);
          }
        });
      }
    } catch (final RejectedExecutionException e) {
      LogManager.instance().log(this, Level.WARNING,
          "Could not submit rollback for abandoned transaction %s; executor already shutting down", e, txCtx.txId);
    } finally {
      txCtx.shutdown();
    }
  }

  public void close() {
    // Stop the idle reaper first so it does not race the shutdown drain below.
    if (txReaper != null)
      txReaper.shutdownNow();

    // Close all open databases
    for (Database db : databasePool.values()) {
      try {
        if (db != null && db.isOpen()) {
          db.close();
        }
      } catch (Exception e) {
        LogManager.instance().log(this, Level.SEVERE, "Error closing database", e);
      }
    }
    databasePool.clear();

    // Clean up transactions - shutdown executors and rollback
    for (TransactionContext txCtx : activeTransactions.values()) {
      try {
        if (txCtx.db != null && txCtx.db.isOpen()) {
          // Execute rollback on the transaction's thread
          txCtx.executor.submit(() -> {
            try {
              txCtx.db.rollback();
            } catch (Exception e) {
              LogManager.instance().log(this, Level.WARNING, "Failed to rollback transaction %s during shutdown", e,
                  txCtx.txId);
            }
          }).get();
        }
      } catch (Exception e) {
        LogManager.instance().log(this, Level.SEVERE, "Error closing transaction", e);
      } finally {
        txCtx.shutdown();
      }
    }
    activeTransactions.clear();
  }

  @Override
  public void executeCommand(ExecuteCommandRequest req, StreamObserver<ExecuteCommandResponse> resp) {

    final long t0 = System.nanoTime();

    try {
      // Check if this is an externally-managed transaction (started via beginTransaction RPC)
      final boolean hasTx = req.hasTransaction();
      final var tx = hasTx ? req.getTransaction() : null;
      final String incomingTxId = hasTx && tx != null ? tx.getTransactionId() : null;
      final TransactionContext txCtx = resolveAuthorizedTransaction(incomingTxId, req.getCredentials());

      if (isUnknownSuppliedTransaction(incomingTxId, txCtx)) {
        // The client supplied a transaction id that is no longer active (e.g. reaped). Reject instead of
        // silently auto-committing this write outside the transaction the client believes it is in.
        resp.onError(unknownTransactionStatus(incomingTxId).asException());
        return;
      }

      if (txCtx != null) {
        // External transaction - execute command on the transaction's dedicated thread
        LogManager.instance().log(this, Level.FINE,
            "executeCommand(): using external transaction %s, executing on dedicated thread", incomingTxId);

        Future<ExecuteCommandResponse> future = txCtx.executor.submit(() ->
            executeCommandInternal(req, t0, txCtx.db, true));

        ExecuteCommandResponse response = future.get();
        resp.onNext(response);
        resp.onCompleted();
      } else {
        // No external transaction - execute on current thread
        Database db = getDatabase(req.getDatabase(), req.getCredentials());
        ExecuteCommandResponse response = executeCommandInternal(req, t0, db, false);
        resp.onNext(response);
        resp.onCompleted();
      }

    } catch (Exception e) {
      // Unwrap ExecutionException to get the root cause
      Throwable cause = e;
      if (e instanceof ExecutionException && e.getCause() != null) {
        cause = e.getCause();
      }
      LogManager.instance().log(this, Level.SEVERE, "ERROR in executeCommand", cause);

      final long ms = (System.nanoTime() - t0) / 1_000_000L;
      ExecuteCommandResponse err = ExecuteCommandResponse
          .newBuilder()
          .setSuccess(false)
          .setMessage(cause.getMessage() == null ? cause.toString() : cause.getMessage())
          .setAffectedRecords(0L)
          .setExecutionTimeMs(ms)
          .build();
      resp.onNext(err);
      resp.onCompleted();
    }
  }

  /**
   * Internal implementation of executeCommand that runs on the appropriate thread.
   *
   * @param req                   the command request
   * @param t0                    start time in nanos
   * @param db                    the database to use
   * @param isExternalTransaction true if this is part of an externally-managed transaction
   * @return the response
   */
  private ExecuteCommandResponse executeCommandInternal(ExecuteCommandRequest req, long t0,
                                                        Database db, boolean isExternalTransaction) {

    boolean beganHere = false;
    final QueryProfile profile = new QueryProfile();
    QueryProfile.pushCurrent(profile);
    ProtocolContext.set("grpc");
    String profileLanguage = null;

    try {
      final long deserStart = System.nanoTime();
      final Map<String, Object> params = GrpcTypeConverter.convertParameters(req.getParametersMap());

      final String language = langOrDefault(req.getLanguage());
      profileLanguage = language;
      profile.addDeserializationNanos(System.nanoTime() - deserStart);

      // Transaction: begin if requested
      final boolean hasTx = req.hasTransaction();
      final var tx = hasTx ? req.getTransaction() : null;

      LogManager.instance().log(this, Level.FINE, "executeCommandInternal(): hasTx = %s tx = %s isExternal = %s",
          hasTx, tx, isExternalTransaction);

      // Auto-wrap in a tx if the client didn't send a tx and none is active (only for non-external)
      final boolean activeAtEntry = db.isTransactionActive();
      final boolean autoWrap = !isExternalTransaction && !hasTx && !activeAtEntry;

      if (isExternalTransaction) {
        // Transaction already started via beginTransaction() - don't begin again
        LogManager.instance().log(this, Level.FINE,
            "executeCommandInternal(): external transaction - tx already active, not beginning");
      } else if (hasTx && tx.getBegin()) {
        db.begin();
        beganHere = true;
      } else if (autoWrap) {
        // Server-side safety: keep UPDATE/UPSERT/MERGE pipelines atomic
        LogManager.instance().log(this, Level.FINE,
            "executeCommandInternal(): auto-wrapping in tx because no client tx and none active");
        db.begin();
        beganHere = true;
      }

      long affected = 0L;

      final boolean returnRows = req.getReturnRows();
      final int maxRows = req.getMaxRows() > 0 ? req.getMaxRows() : DEFAULT_MAX_COMMAND_ROWS;

      ExecuteCommandResponse.Builder out = ExecuteCommandResponse.newBuilder()
          .setSuccess(true)
          .setMessage("OK");

      // Execute the command

      LogManager.instance().log(this, Level.FINE, "executeCommandInternal(): command = %s", req.getCommand());

      long serializationAccum = 0L;
      final long engineStart = System.nanoTime();
      QueryStatistics writeStats = null;

      try (ResultSet rs = db.command(language, req.getCommand(), params)) {

        if (rs != null) {

          LogManager.instance().log(this, Level.FINE, "executeCommandInternal(): rs = %s", rs);

          if (returnRows) {

            LogManager.instance().log(this, Level.FINE, "executeCommandInternal(): returning rows ...");

            int emitted = 0;

            while (rs.hasNext()) {

              Result result = rs.next();

              if (result.isElement()) {
                affected++;
              } else {

                for (String p : result.getPropertyNames()) {
                  Object v = result.getProperty(p);
                  if (v instanceof Number n) {
                    affected += n.longValue();
                  }
                }
              }

              if (emitted < maxRows) {
                final long serRowStart = System.nanoTime();
                // Convert Result to GrpcRecord, preserving aliases and all properties
                GrpcRecord grpcRecord = convertResultToGrpcRecord(result, db,
                    new ProjectionConfig(true, ProjectionEncoding.PROJECTION_AS_JSON, 0));
                out.addRecords(grpcRecord);
                serializationAccum += System.nanoTime() - serRowStart;

                emitted++;
              }

            }
          } else {

            LogManager.instance().log(this, Level.FINE, "executeCommandInternal(): not returning rows ... rs = %s", rs);

            // Not returning rows: still consume to compute 'affected'
            while (rs.hasNext()) {
              Result r = rs.next();
              if (r.isElement()) {
                affected++;
              } else {
                for (String p : r.getPropertyNames()) {
                  Object v = r.getProperty(p);
                  if (v instanceof Number n)
                    affected += n.longValue();
                }
              }
            }
          }

          writeStats = rs.getStatistics().orElse(null);
        }
      }

      final long engineEnd = System.nanoTime();
      profile.addEngineNanos(engineEnd - engineStart - serializationAccum);
      profile.addSerializationNanos(serializationAccum);

      LogManager.instance().log(this, Level.FINE, "executeCommandInternal(): after - hasTx = %s tx = %s", hasTx, tx);

      // Handle transaction commit/rollback
      if (isExternalTransaction) {
        // Transaction was started via beginTransaction() RPC - don't touch its lifecycle
        LogManager.instance().log(this, Level.FINE,
            "executeCommandInternal(): external transaction - skipping auto-commit/rollback");
      } else if (hasTx) {
        // Transaction end — precedence: rollback > commit > begin-only⇒commit
        if (tx.getRollback()) {
          LogManager.instance()
              .log(this, Level.FINE, "executeCommandInternal(): rolling back db=%s tid=%s", db.getName(),
                  tx.getTransactionId());
          db.rollback();
        } else if (tx.getCommit()) {
          LogManager.instance()
              .log(this, Level.FINE, "executeCommandInternal(): committing [tx.getCommit() == true] db=%s tid=%s",
                  db.getName(),
                  tx.getTransactionId());
          db.commit();
        } else if (beganHere && db.isTransactionActive()) {
          // Began but no explicit commit/rollback flag — default to commit (HTTP parity)
          LogManager.instance()
              .log(this, Level.FINE, "executeCommandInternal(): committing [beganHere == true] db=%s tid=%s",
                  db.getName(),
                  tx.getTransactionId());
          db.commit();
        }
      } else if (beganHere && db.isTransactionActive()) {
        // Auto-wrapped tx: ensure we commit after fully draining ResultSet
        LogManager.instance().log(this, Level.FINE, "executeCommandInternal(): committing [autoWrap] db=%s",
            db.getName());
        db.commit();
      }

      final long ms = (System.nanoTime() - t0) / 1_000_000L;
      final long serFinalStart = System.nanoTime();
      out.setAffectedRecords(affected).setExecutionTimeMs(ms);
      if (writeStats != null && writeStats.containsUpdates())
        out.setStats(QueryUpdateStats.newBuilder()
            .setNodesCreated(writeStats.getNodesCreated())
            .setNodesDeleted(writeStats.getNodesDeleted())
            .setRelationshipsCreated(writeStats.getRelationshipsCreated())
            .setRelationshipsDeleted(writeStats.getRelationshipsDeleted())
            .setPropertiesSet(writeStats.getPropertiesSet())
            .setLabelsAdded(writeStats.getLabelsAdded())
            .setLabelsRemoved(writeStats.getLabelsRemoved())
            .setIndexesAdded(writeStats.getIndexesAdded())
            .setIndexesRemoved(writeStats.getIndexesRemoved())
            .setConstraintsAdded(writeStats.getConstraintsAdded())
            .setConstraintsRemoved(writeStats.getConstraintsRemoved())
            .setContainsUpdates(true)
            .build());
      final ExecuteCommandResponse response = out.build();
      profile.addSerializationNanos(System.nanoTime() - serFinalStart);
      return response;

    } catch (Exception e) {

      LogManager.instance().log(this, Level.SEVERE, "ERROR in executeCommandInternal", e);

      // Best-effort rollback if we began here and failed (only for non-external transactions)
      try {
        if (beganHere && db != null && !isExternalTransaction) {
          db.rollback();
        } else if (isExternalTransaction) {
          LogManager.instance().log(this, Level.FINE,
              "executeCommandInternal(): error occurred but external transaction - skipping auto-rollback");
        }
      } catch (Exception ignore) {
        /* no-op */
      }

      final long ms = (System.nanoTime() - t0) / 1_000_000L;
      return ExecuteCommandResponse
          .newBuilder()
          .setSuccess(false)
          .setMessage(e.getMessage() == null ? e.toString() : e.getMessage())
          .setAffectedRecords(0L)
          .setExecutionTimeMs(ms)
          .build();
    } finally {
      ProtocolContext.clear();
      recordGrpcProfile("grpc.command", profile, db != null ? db.getName() : req.getDatabase(),
          profileLanguage != null ? profileLanguage : req.getLanguage(), req.getCommand());
      QueryProfile.popCurrent();
    }
  }

  private void recordGrpcProfile(final String metricPrefix, final QueryProfile profile, final String databaseName,
                                 final String language, final String queryText) {
    Metrics.counter(metricPrefix).increment();
    Metrics.timer(metricPrefix + ".deserialization").record(profile.getDeserializationNanos(), TimeUnit.NANOSECONDS);
    Metrics.timer(metricPrefix + ".engine").record(profile.getEngineNanos(), TimeUnit.NANOSECONDS);
    Metrics.timer(metricPrefix + ".serialization").record(profile.getSerializationNanos(), TimeUnit.NANOSECONDS);

    if (arcadeServer == null)
      return;
    final ServerQueryProfiler serverProfiler = arcadeServer.getQueryProfiler();
    if (serverProfiler == null || !serverProfiler.isRecording())
      return;
    serverProfiler.recordQuery(databaseName, language, queryText, profile, null);
  }

  @Override
  public void createRecord(CreateRecordRequest req, StreamObserver<CreateRecordResponse> resp) {
    // Check for external transaction
    final String incomingTxId = req.hasTransaction() ? req.getTransaction().getTransactionId() : null;
    final TransactionContext txCtx;
    try {
      txCtx = resolveAuthorizedTransaction(incomingTxId, req.getCredentials());
    } catch (final StatusRuntimeException e) {
      resp.onError(e);
      return;
    }

    if (isUnknownSuppliedTransaction(incomingTxId, txCtx)) {
      // Non-blank transaction id the server no longer knows about (e.g. reaped): fail loudly instead of
      // silently auto-committing this write outside the transaction the client believes it is in.
      resp.onError(unknownTransactionStatus(incomingTxId).asException());
      return;
    }

    if (txCtx != null) {
      // External transaction — execute on its dedicated thread to maintain thread-local state
      try {
        final Future<CreateRecordResponse> future = txCtx.executor.submit(() -> createRecordInternal(req, txCtx.db));
        resp.onNext(future.get());
        resp.onCompleted();
      } catch (Exception e) {
        final Throwable cause = e instanceof ExecutionException && e.getCause() != null ? e.getCause() : e;
        LogManager.instance().log(this, Level.SEVERE, "ERROR in createRecord (external tx)", cause);
        // Preserve the engine exception type (e.g. DuplicatedKeyException -> ALREADY_EXISTS with index/keys)
        // so the client can reconstruct it instead of receiving an opaque INTERNAL.
        resp.onError(GrpcErrorMapper.toStatusRuntimeException(cause, "CreateRecord"));
      }
      return;
    }

    // No external transaction — inline per-call handling
    try {
      final Database db = getDatabase(req.getDatabase(), req.getCredentials());
      resp.onNext(createRecordInternal(req, db));
      resp.onCompleted();
    } catch (Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "ERROR in createRecord", e);
      resp.onError(GrpcErrorMapper.toStatusRuntimeException(e, "CreateRecord"));
    }
  }

  private CreateRecordResponse createRecordInternal(final CreateRecordRequest req, final Database db) {
    final String cls = req.getType();
    if (cls == null || cls.isEmpty())
      throw new IllegalArgumentException("targetClass is required");

    final Schema schema = db.getSchema();
    final DocumentType dt = schema.getType(cls);
    if (dt == null)
      throw new IllegalArgumentException("Class not found: " + cls);

    // All properties from the request (proto map) — nested under "record"
    final Map<String, GrpcValue> props = req.getRecord().getPropertiesMap();

    final boolean beganHere = !db.isTransactionActive();
    if (beganHere)
      db.begin();

    try {
      // --- Vertex ---
      if (dt instanceof VertexType) {
        final MutableVertex v = db.newVertex(cls);
        props.forEach((k, val) -> v.set(k, toJavaForProperty(db, v, dt, k, val)));
        v.save();

        if (beganHere)
          db.commit();

        return CreateRecordResponse.newBuilder().setRid(v.getIdentity().toString()).build();
      }

      // --- Edge ---
      if (dt instanceof EdgeType) {
        String outStr = null, inStr = null;

        if (props.containsKey("out")) {
          GrpcValue pv = props.get("out");
          outStr = pv.getKindCase() == GrpcValue.KindCase.STRING_VALUE ? pv.getStringValue() :
              String.valueOf(fromGrpcValue(pv));
        }

        if (props.containsKey("in")) {
          GrpcValue pv = props.get("in");
          inStr = pv.getKindCase() == GrpcValue.KindCase.STRING_VALUE ? pv.getStringValue() :
              String.valueOf(fromGrpcValue(pv));
        }

        if (outStr == null || inStr == null)
          throw new IllegalArgumentException("Edge requires 'out' and 'in' RIDs");

        final var outEl = db.lookupByRID(new RID(outStr), false);
        final var inEl = db.lookupByRID(new RID(inStr), false);
        final Vertex outV = outEl.asVertex(false);

        final MutableEdge e = outV.newEdge(cls, inEl);
        props.forEach((k, val) -> {
          if (!"out".equals(k) && !"in".equals(k))
            e.set(k, toJavaForProperty(db, e, dt, k, val));
        });
        e.save();

        if (beganHere)
          db.commit();

        return CreateRecordResponse.newBuilder().setRid(e.getIdentity().toString()).build();
      }

      // --- Document ---
      final MutableDocument d = db.newDocument(cls);
      props.forEach((k, val) -> d.set(k, toJavaForProperty(db, d, dt, k, val)));
      d.save();

      if (beganHere)
        db.commit();

      return CreateRecordResponse.newBuilder().setRid(d.getIdentity().toString()).build();
    } catch (Exception e) {
      if (beganHere) {
        try { db.rollback(); } catch (Exception ignore) { }
      }
      throw e;
    }
  }

  @Override
  public void lookupByRid(LookupByRidRequest req, StreamObserver<LookupByRidResponse> resp) {
    // When the read is part of an externally-managed transaction, execute it on the transaction's dedicated thread so the
    // record version is tracked in that transaction. Under REPEATABLE_READ this is what lets a later write detect a
    // concurrent modification on commit, mirroring the HTTP behavior (issue #4533).
    final String incomingTxId = req.hasTransaction() ? req.getTransaction().getTransactionId() : null;
    final TransactionContext txCtx;
    try {
      txCtx = resolveAuthorizedTransaction(incomingTxId, req.getCredentials());
    } catch (final StatusRuntimeException e) {
      resp.onError(e);
      return;
    }

    if (isUnknownSuppliedTransaction(incomingTxId, txCtx)) {
      // Non-blank transaction id the server no longer knows about (e.g. reaped): fail loudly instead of
      // silently auto-committing this write outside the transaction the client believes it is in.
      resp.onError(unknownTransactionStatus(incomingTxId).asException());
      return;
    }

    if (txCtx != null) {
      try {
        final Future<LookupByRidResponse> future = txCtx.executor.submit(() -> lookupByRidInternal(req, txCtx.db));
        resp.onNext(future.get());
        resp.onCompleted();
      } catch (Exception e) {
        final Throwable cause = e instanceof ExecutionException && e.getCause() != null ? e.getCause() : e;
        if (cause instanceof RecordNotFoundException)
          resp.onError(Status.NOT_FOUND.withDescription("LookupByRid: " + cause.getMessage()).asException());
        else
          resp.onError(Status.INTERNAL.withDescription("LookupByRid: " + cause.getMessage()).asException());
      }
      return;
    }

    try {
      final Database db = getDatabase(req.getDatabase(), req.getCredentials());
      resp.onNext(lookupByRidInternal(req, db));
      resp.onCompleted();
    } catch (RecordNotFoundException e) {
      resp.onError(Status.NOT_FOUND.withDescription("LookupByRid: " + e.getMessage()).asException());
    } catch (Exception e) {
      resp.onError(Status.INTERNAL.withDescription("LookupByRid: " + e.getMessage()).asException());
    }
  }

  private LookupByRidResponse lookupByRidInternal(final LookupByRidRequest req, final Database db) {
    final String ridStr = req.getRid();
    if (ridStr == null || ridStr.isBlank())
      throw new IllegalArgumentException("rid is required");

    final var el = db.lookupByRID(new RID(ridStr), true);

    return LookupByRidResponse.newBuilder().setFound(true).setRecord(convertToGrpcRecord(el.getRecord(), db)).build();
  }

  @Override
  public void updateRecord(final UpdateRecordRequest req, final StreamObserver<UpdateRecordResponse> resp) {
    // Check for external transaction
    final String incomingTxId = req.hasTransaction() ? req.getTransaction().getTransactionId() : null;
    final TransactionContext txCtx;
    try {
      txCtx = resolveAuthorizedTransaction(incomingTxId, req.getCredentials());
    } catch (final StatusRuntimeException e) {
      resp.onError(e);
      return;
    }

    if (isUnknownSuppliedTransaction(incomingTxId, txCtx)) {
      // Non-blank transaction id the server no longer knows about (e.g. reaped): fail loudly instead of
      // silently auto-committing this write outside the transaction the client believes it is in.
      resp.onError(unknownTransactionStatus(incomingTxId).asException());
      return;
    }

    if (txCtx != null) {
      // External transaction — execute on its dedicated thread to maintain thread-local state
      try {
        final Future<UpdateRecordResponse> future = txCtx.executor.submit(() -> updateRecordInternal(req, txCtx.db));
        resp.onNext(future.get());
        resp.onCompleted();
      } catch (Exception e) {
        final Throwable cause = e instanceof ExecutionException && e.getCause() != null ? e.getCause() : e;
        LogManager.instance().log(this, Level.SEVERE, "ERROR in updateRecord (external tx)", cause);
        if (cause instanceof RecordNotFoundException)
          resp.onError(Status.NOT_FOUND.withDescription("Record not found: " + req.getRid()).asException());
        else
          resp.onError(Status.INTERNAL.withDescription("UpdateRecord: " + cause.getMessage()).asException());
      }
      return;
    }

    // No external transaction — inline per-call handling
    try {
      final Database db = getDatabase(req.getDatabase(), req.getCredentials());
      resp.onNext(updateRecordInternal(req, db));
      resp.onCompleted();
    } catch (RecordNotFoundException e) {
      resp.onError(Status.NOT_FOUND.withDescription("Record not found: " + req.getRid()).asException());
    } catch (Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "ERROR in updateRecord", e);
      resp.onError(Status.INTERNAL.withDescription("UpdateRecord: " + e.getMessage()).asException());
    }
  }

  private UpdateRecordResponse updateRecordInternal(final UpdateRecordRequest req, final Database db) {
    boolean beganHere = false;

    String ridStr = null;
    try {
      ridStr = req.getRid();

      if (ridStr == null || ridStr.isEmpty())
        throw new IllegalArgumentException("rid is required");

      // Begin transaction if requested by inline tx flags (not external transaction)
      final boolean hasTx = req.hasTransaction();
      final var tx = hasTx ? req.getTransaction() : null;

      if (hasTx && tx.getBegin()) {
        db.begin();
        beganHere = true;
      } else if (!db.isTransactionActive()) {
        db.begin();
        beganHere = true;
      }

      // Lookup the record by RID. BaseRecord upgrades the stored identity to a DatabaseRID at construction, so modify()/getPageId() stay correct.
      var el = db.lookupByRID(new RID(ridStr), true);

      final var dbRef = db;

      LogManager.instance().log(this, Level.FINE, "updateRecord(): el = %s; type = %s", el, el.getRecordType());

      if (el instanceof Vertex elAsVertex) {

        LogManager.instance().log(this, Level.FINE, "updateRecord(): Processing Vertex ...");

        // Get mutable view for updates (works for docs, vertices, edges)
        MutableVertex mvertex = elAsVertex.modify();

        var dtype = db.getSchema().getType(mvertex.getTypeName());

        // Apply updates

        final Map<String, GrpcValue> props = req.hasRecord() ? req.getRecord().getPropertiesMap()
            : req.hasPartial() ? req.getPartial().getPropertiesMap() : Collections.emptyMap();

        // In full-replacement mode (oneof "record") remove the properties that are no longer present, so a
        // client-side save() that dropped a property is mirrored on the server (matches the HTTP "update content"
        // semantics). In partial mode (oneof "partial") only the provided keys are merged.
        if (req.hasRecord()) {
          for (final String existing : new ArrayList<>(mvertex.getPropertyNames()))
            if (!props.containsKey(existing))
              mvertex.remove(existing);
        }

        // Exclude ArcadeDB system fields during update

        props.forEach((k, v) -> {
          String key = k.trim().toLowerCase();
          if ("@rid".equals(key) || "@type".equals(key) || "@cat".equals(key)) {
            // Skip internal fields to prevent accidental overwrites
            LogManager.instance().log(this, Level.FINE, "Skipping internal field during update: %s", k);
            return;
          }
          // Perform the update for user-defined fields
          mvertex.set(k, toJavaForProperty(dbRef, mvertex, dtype, k, v));
        });

        mvertex.save();
      } else if (el instanceof Document elAsDocument) {

        LogManager.instance().log(this, Level.FINE, "updateRecord(): Processing Document ...");

        MutableDocument mdoc = elAsDocument.modify();

        var dtype = db.getSchema().getType(mdoc.getTypeName());

        // Apply updates

        final Map<String, GrpcValue> props = req.hasRecord() ? req.getRecord().getPropertiesMap()
            : req.hasPartial() ? req.getPartial().getPropertiesMap() : Collections.emptyMap();

        // In full-replacement mode (oneof "record") remove the properties that are no longer present, so a
        // client-side save() that dropped a property is mirrored on the server (matches the HTTP "update content"
        // semantics). In partial mode (oneof "partial") only the provided keys are merged.
        if (req.hasRecord()) {
          for (final String existing : new ArrayList<>(mdoc.getPropertyNames()))
            if (!props.containsKey(existing))
              mdoc.remove(existing);
        }

        // Exclude ArcadeDB system fields during update

        props.forEach((k, v) -> {
          String key = k.trim().toLowerCase();
          if ("@rid".equals(key) || "@type".equals(key) || "@cat".equals(key)) {
            // Skip internal fields to prevent accidental overwrites
            LogManager.instance().log(this, Level.FINE, "Skipping internal field during update: %s", k);
            return;
          }
          // Perform the update for user-defined fields
          mdoc.set(k, toJavaForProperty(dbRef, mdoc, dtype, k, v));
        });

        mdoc.save();
      }

      // Commit/rollback with proper precedence (only for per-call transactions, not external)
      if (beganHere) {
        if (hasTx && tx.getRollback())
          db.rollback();
        else
          db.commit();
      }

      return UpdateRecordResponse.newBuilder().setUpdated(true).setSuccess(true).build();
    } catch (RecordNotFoundException e) {
      if (beganHere) {
        try { db.rollback(); } catch (Exception ignore) { }
      }
      throw e;
    } catch (Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "ERROR in updateRecord", e);
      if (beganHere) {
        try { db.rollback(); } catch (Exception ignore) { }
      }
      throw e;
    }
  }

  @Override
  public void deleteRecord(DeleteRecordRequest req, StreamObserver<DeleteRecordResponse> resp) {
    // Check for external transaction
    final String incomingTxId = req.hasTransaction() ? req.getTransaction().getTransactionId() : null;
    final TransactionContext txCtx;
    try {
      txCtx = resolveAuthorizedTransaction(incomingTxId, req.getCredentials());
    } catch (final StatusRuntimeException e) {
      resp.onError(e);
      return;
    }

    if (isUnknownSuppliedTransaction(incomingTxId, txCtx)) {
      // Non-blank transaction id the server no longer knows about (e.g. reaped): fail loudly instead of
      // silently auto-committing this write outside the transaction the client believes it is in.
      resp.onError(unknownTransactionStatus(incomingTxId).asException());
      return;
    }

    if (txCtx != null) {
      // External transaction — execute on its dedicated thread to maintain thread-local state
      try {
        final Future<DeleteRecordResponse> future = txCtx.executor.submit(() -> deleteRecordInternal(req, txCtx.db));
        resp.onNext(future.get());
        resp.onCompleted();
      } catch (Exception e) {
        final Throwable cause = e instanceof ExecutionException && e.getCause() != null ? e.getCause() : e;
        LogManager.instance().log(this, Level.SEVERE, "ERROR in deleteRecord (external tx)", cause);
        if (cause instanceof RecordNotFoundException)
          resp.onError(Status.NOT_FOUND.withDescription("Record not found: " + req.getRid()).asException());
        else
          resp.onError(Status.INTERNAL.withDescription("DeleteRecord: " + cause.getMessage()).asException());
      }
      return;
    }

    // No external transaction — inline per-call handling
    try {
      final Database db = getDatabase(req.getDatabase(), req.getCredentials());
      resp.onNext(deleteRecordInternal(req, db));
      resp.onCompleted();
    } catch (RecordNotFoundException e) {
      resp.onError(Status.NOT_FOUND.withDescription("Record not found: " + req.getRid()).asException());
    } catch (Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "ERROR in deleteRecord", e);
      resp.onError(
          Status.INTERNAL.withDescription("DeleteRecord: " + (e.getMessage() == null ? e.toString() : e.getMessage()))
              .asException());
    }
  }

  private DeleteRecordResponse deleteRecordInternal(final DeleteRecordRequest req, final Database db) {
    final String ridStr = req.getRid();
    if (ridStr == null || ridStr.isBlank())
      throw new IllegalArgumentException("rid is required");

    final boolean hasTx = req.hasTransaction();
    final boolean beganHere;

    if (hasTx && req.getTransaction().getBegin()) {
      db.begin();
      beganHere = true;
    } else if (!db.isTransactionActive()) {
      db.begin();
      beganHere = true;
    } else {
      beganHere = false;
    }

    try {
      final var el = db.lookupByRID(new RID(ridStr), false);
      el.delete();

      if (beganHere) {
        if (hasTx && req.getTransaction().getRollback())
          db.rollback();
        else
          db.commit();
      }

      return DeleteRecordResponse.newBuilder().setSuccess(true).setDeleted(true).build();
    } catch (Exception e) {
      if (beganHere) {
        try { db.rollback(); } catch (Exception ignore) { }
      }
      throw e;
    }
  }

  @Override
  public void executeQuery(ExecuteQueryRequest request, StreamObserver<ExecuteQueryResponse> responseObserver) {
    // Force compression for streaming (usually beneficial)
    CompressionAwareService.setResponseCompression(responseObserver, "gzip");

    // If the client is inside an externally-managed transaction (started via beginTransaction RPC),
    // route the query to that transaction's dedicated executor thread. ArcadeDB transactions are
    // thread-bound: changes pending in the transaction (e.g. a record created via createRecord or
    // executeCommand and not yet committed) are only visible to the thread that owns the
    // transaction. Running the query on the gRPC worker thread would miss those in-flight
    // changes and yield "Record not found" for a record the client just saved (issue #4260).
    if (request.hasTransaction()) {
      final String incomingTxId = request.getTransaction().getTransactionId();
      LogManager.instance().log(this, Level.FINE, "executeQuery(): has Tx %s", incomingTxId);

      final TransactionContext txCtx;
      try {
        txCtx = resolveAuthorizedTransaction(incomingTxId, request.getCredentials());
      } catch (final StatusRuntimeException e) {
        // Preserve the authz/authn status (PERMISSION_DENIED/UNAUTHENTICATED) instead of masking it.
        responseObserver.onError(e);
        return;
      }
      if (txCtx == null) {
        // Unknown/expired transaction id is a client-side precondition failure, not a server fault, so the
        // client can tell a programming error apart from an INTERNAL error.
        responseObserver.onError(Status.FAILED_PRECONDITION
            .withDescription("Query execution failed: Invalid transaction ID").asException());
        return;
      }

      try {
        final Future<ExecuteQueryResponse> future = txCtx.executor.submit(
            () -> executeQueryInternal(request, txCtx.db));
        final ExecuteQueryResponse response = future.get();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
      } catch (Exception e) {
        final Throwable cause = e instanceof ExecutionException && e.getCause() != null ? e.getCause() : e;
        if (cause instanceof StatusRuntimeException sre) {
          // Preserve an explicit gRPC status (e.g. RESOURCE_EXHAUSTED from the result cap, or the
          // authz/authn status from getDatabase) instead of masking it as INTERNAL.
          LogManager.instance().log(this, Level.FINE, "Query rejected: %s", sre, sre.getMessage());
          responseObserver.onError(sre);
        } else {
          LogManager.instance().log(this, Level.SEVERE, "Error executing query: %s", cause, cause.getMessage());
          responseObserver.onError(Status.INTERNAL
              .withDescription("Query execution failed: " + cause.getMessage()).asException());
        }
      }
      return;
    }

    // No external transaction: run on the gRPC worker thread.
    Database database = null;
    try {
      database = getDatabase(request.getDatabase(), request.getCredentials());
      final ExecuteQueryResponse response = executeQueryInternal(request, database);
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (final StatusRuntimeException e) {
      // Preserve the status code chosen by getDatabase (e.g. INVALID_ARGUMENT for a rejected
      // database name, PERMISSION_DENIED/UNAUTHENTICATED for authz/authn failures) instead of
      // masking it as INTERNAL.
      LogManager.instance().log(this, Level.FINE, "Query rejected: %s", e, e.getMessage());
      responseObserver.onError(e);
    } catch (Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Error executing query: %s", e, e.getMessage());
      responseObserver.onError(Status.INTERNAL.withDescription("Query execution failed: " + e.getMessage()).asException());
    }
  }

  private ExecuteQueryResponse executeQueryInternal(final ExecuteQueryRequest request, final Database database) {
    final QueryProfile profile = new QueryProfile();
    QueryProfile.pushCurrent(profile);
    ProtocolContext.set("grpc");
    String profileLanguage = null;
    try {
      final long deserStart = System.nanoTime();
      ProjectionConfig projectionConfig = getProjectionConfig(request);

      LogManager.instance().log(this, Level.FINE, """
              executeQuery(): projectionConfig.include = %s projectionConfig\
              .mode = %s""",
          projectionConfig.isInclude(),
          projectionConfig.getEnc());

      // Execute the query
      long startTime = System.currentTimeMillis();

      final String language = langOrDefault(request.getLanguage());
      profileLanguage = language;

      LogManager.instance().log(this, Level.FINE, "executeQuery(): language = %s query = %s", language, request.getQuery());

      final Map<String, Object> queryParams = GrpcTypeConverter.convertParameters(request.getParametersMap());
      profile.addDeserializationNanos(System.nanoTime() - deserStart);

      final long engineStart = System.nanoTime();
      long serializationAccum = 0L;
      try (final ResultSet resultSet = database.query(language, request.getQuery(), queryParams)) {

        LogManager.instance()
            .log(this, Level.FINE, "executeQuery(): to get resultSet = %s", System.currentTimeMillis() - startTime);

        // Build response
        QueryResult.Builder resultBuilder = QueryResult.newBuilder();

        // Process results
        int count = 0;

        // Bound the materialized response. An explicit positive limit at or below the configured cap is the
        // client's own bound and is honored silently. The configured cap (when > 0) is otherwise a HARD ceiling:
        // a client cannot bypass DoS protection by requesting a larger limit, and a result that exceeds the cap
        // fails loudly with RESOURCE_EXHAUSTED - consistent with the MATERIALIZE_ALL stream path - instead of
        // silently truncating and dropping data without telling the caller.
        final int requestedLimit = request.getLimit();
        final int configuredMax = GlobalConfiguration.SERVER_GRPC_QUERY_MAX_RESULT_ROWS.getValueAsInteger();
        final boolean capEnabled = configuredMax > 0;
        // The client's explicit limit applies as its own bound only when it does not exceed the hard ceiling.
        final boolean clientLimitWithinCap = requestedLimit > 0 && (!capEnabled || requestedLimit <= configuredMax);

        LogManager.instance().log(this, Level.FINE, "executeQuery(): resultSet.size = %s",
            resultSet.getExactSizeIfKnown());

        while (resultSet.hasNext()) {

          Result result = resultSet.next();

          LogManager.instance().log(this, Level.FINE, "executeQuery(): result = %s", result);

          final long serRowStart = System.nanoTime();
          // Convert Result to GrpcRecord, preserving aliases and all properties
          GrpcRecord grpcRecord = convertResultToGrpcRecord(result, database, projectionConfig);

          LogManager.instance().log(this, Level.FINE, "executeQuery(): grpcRecord -> @rid = %s", grpcRecord.getRid());

          resultBuilder.addRecords(grpcRecord);
          serializationAccum += System.nanoTime() - serRowStart;

          count++;

          // Honor the client's explicit limit (guaranteed not to exceed the hard ceiling).
          if (clientLimitWithinCap && count >= requestedLimit)
            break;

          // Hard ceiling reached with more rows still available: fail loudly so the caller is never silently
          // truncated and cannot bypass the cap with an oversized limit.
          if (capEnabled && count >= configuredMax && resultSet.hasNext())
            throw Status.RESOURCE_EXHAUSTED
                .withDescription("ExecuteQuery result exceeds the maximum of " + configuredMax
                    + " rows (arcadedb.server.grpcQueryMaxResultRows); add a LIMIT or use StreamQuery with CURSOR/PAGED retrieval mode for large results")
                .asRuntimeException();
        }
        profile.addEngineNanos(System.nanoTime() - engineStart - serializationAccum);

        LogManager.instance().log(this, Level.FINE, "executeQuery(): count = %s", count);

        final long serBuildStart = System.nanoTime();
        resultBuilder.setTotalRecordsInBatch(count);

        long executionTime = System.currentTimeMillis() - startTime;

        ExecuteQueryResponse response = ExecuteQueryResponse.newBuilder().addResults(resultBuilder.build())
            .setExecutionTimeMs(executionTime)
            .build();
        profile.addSerializationNanos(serializationAccum + (System.nanoTime() - serBuildStart));

        LogManager.instance().log(this, Level.FINE, "executeQuery(): executionTime + response generation = %s",
            executionTime);

        return response;
      }
    } finally {
      ProtocolContext.clear();
      recordGrpcProfile("grpc.query", profile, database != null ? database.getName() : request.getDatabase(),
          profileLanguage != null ? profileLanguage : request.getLanguage(), request.getQuery());
      QueryProfile.popCurrent();
    }
  }

  private @NonNull ProjectionConfig getProjectionConfig(ExecuteQueryRequest request) {
    ProjectionConfig projectionConfig;
    if (request.hasProjectionSettings()) {

      var projectionSettings = request.getProjectionSettings();

      final boolean includeProjections = projectionSettings.getIncludeProjections();
      final var projMode = projectionSettings.getProjectionEncoding();

      int softLimit = projectionSettings.hasSoftLimitBytes() ?
          projectionSettings.getSoftLimitBytes().getValue() :
          0; // choose your
      // default
      // or 0 =
      // unlimited
      projectionConfig = new ProjectionConfig(includeProjections, projMode, softLimit);
    } else {

      projectionConfig = new ProjectionConfig(true, ProjectionEncoding.PROJECTION_AS_JSON, 0);
    }
    return projectionConfig;
  }

  /**
   * Maps the wire-protocol {@link TransactionIsolation} to the engine {@link Database.TRANSACTION_ISOLATION_LEVEL}.
   * ArcadeDB only supports READ_COMMITTED and REPEATABLE_READ, so weaker/stronger levels collapse to the closest match.
   * Unset (proto default, old clients) maps to READ_COMMITTED to preserve the previous behavior.
   */
  private static Database.TRANSACTION_ISOLATION_LEVEL mapIsolationLevel(final TransactionIsolation isolation) {
    if (isolation == null)
      return Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED;
    return switch (isolation) {
      case REPEATABLE_READ, SERIALIZABLE -> Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ;
      default -> Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED;
    };
  }

  @Override
  public void beginTransaction(BeginTransactionRequest request,
                               StreamObserver<BeginTransactionResponse> responseObserver) {
    final String reqDb = request.getDatabase();
    final String user = request.hasCredentials() ? request.getCredentials().getUsername() : null;

    LogManager.instance()
        .log(this, Level.FINE, "beginTransaction(): received request db=%s user=%s activeTxCount(before)=%s", reqDb,
            user != null ? user : "<none>",
            activeTransactions.size());

    // Declare txCtx outside try block so we can clean it up on failure
    TransactionContext txCtx = null;
    // Track whether we hold a reserved concurrency slot so the failure path releases it exactly once.
    boolean reserved = false;
    String owner = null;

    try {
      final Database database = getDatabase(reqDb, request.getCredentials());

      LogManager.instance().log(this, Level.FINE, """
              beginTransaction(): resolved database instance dbName=%s class=%s \
              hash=%s""",
          database != null ? database.getName() : "<null>", database != null ?
              database.getClass().getSimpleName() : "<null>",
          database != null ? System.identityHashCode(database) : 0);

      // Generate transaction ID first so we can create the context
      final String transactionId = generateTransactionId();

      // Bind the transaction to the authenticated principal so a transaction-scoped RPC can later reject
      // any caller other than the owner (getDatabase() above already authenticated/authorized this user).
      owner = resolvedUsername(request.getCredentials());

      // Enforce the concurrent-transaction caps BEFORE allocating the dedicated executor thread, so a
      // beginTransaction flood is rejected without ever creating the thread it is trying to exhaust.
      if (!tryReserveTransactionSlot(owner)) {
        // Report the configured caps rather than live counts: tryReserveTransactionSlot has already rolled back its
        // increments on rejection, so a live read here would be post-decrement and misleadingly below the cap.
        LogManager.instance().log(this, Level.WARNING,
            "beginTransaction(): rejected for principal=%s - concurrent transaction limit reached (caps: global=%s, perPrincipal=%s)",
            owner != null ? owner : "<anonymous>", maxConcurrentTransactions, maxConcurrentTransactionsPerPrincipal);
        responseObserver.onError(Status.RESOURCE_EXHAUSTED
            .withDescription("Too many concurrent transactions; retry after committing or rolling back open transactions")
            .asException());
        return;
      }
      reserved = true;

      // Create transaction context with dedicated executor thread
      txCtx = new TransactionContext(database, transactionId, owner);

      LogManager.instance().log(this, Level.FINE, """
          beginTransaction(): calling database.begin() on dedicated thread \
          for txId=%s""", transactionId);

      // Honor the isolation level requested by the client (defaults to READ_COMMITTED). This is required so that
      // REPEATABLE_READ transactions track the version of the records they read and a write-write conflict raises a
      // ConcurrentModificationException on commit, exactly as on HTTP (issue #4533).
      final Database.TRANSACTION_ISOLATION_LEVEL isolationLevel = mapIsolationLevel(request.getIsolation());

      // Begin transaction ON THE DEDICATED THREAD - this is critical because ArcadeDB
      // transactions are thread-local
      Future<?> beginFuture = txCtx.executor.submit(() -> {
        // Initialize the DatabaseContext on this dedicated thread before any DB operation
        DatabaseContext.INSTANCE.init((DatabaseInternal) database);
        database.begin(isolationLevel);
      });
      beginFuture.get(); // Wait for begin to complete

      // Register the transaction context. putIfAbsent (never plain put) so two concurrent begins can never
      // silently overwrite one another's context; with UUID ids a collision is effectively impossible, so a
      // non-null return is treated as an internal error. Roll back the transaction we just started on its
      // own thread before failing so it does not leak.
      final TransactionContext registered = txCtx;
      final TransactionContext existing = activeTransactions.putIfAbsent(transactionId, registered);
      if (existing != null) {
        try {
          registered.executor.submit(() -> database.rollback()).get();
        } catch (final Exception rollbackError) {
          LogManager.instance().log(this, Level.WARNING,
              "Failed to roll back transaction after id collision for txId=%s", rollbackError, transactionId);
        }
        throw new IllegalStateException("Transaction id collision for id=" + transactionId);
      }

      // Registration succeeded: the reserved slot and the executor now belong to the live transaction, which is
      // torn down (and its slot released) by commit/rollback/reap. Clear the local cleanup handles so the catch
      // block below cannot double-release the slot or shut down the live transaction's executor.
      txCtx = null;
      reserved = false;

      LogManager.instance()
          .log(this, Level.FINE, "beginTransaction(): started txId=%s for db=%s activeTxCount(after)=%s", transactionId,
              database != null ? database.getName() : "<null>", activeTransactions.size());

      final BeginTransactionResponse response = BeginTransactionResponse.newBuilder().setTransactionId(transactionId)
          .setTimestamp(System.currentTimeMillis()).build();

      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Throwable t) {
      // Shutdown the executor if it was created but not registered (prevents thread leak)
      if (txCtx != null) {
        txCtx.shutdown();
      }
      // Release the reserved concurrency slot if the transaction never became live.
      if (reserved) {
        releaseTransactionSlot(owner);
      }
      Throwable cause = t instanceof ExecutionException && t.getCause() != null ? t.getCause() : t;
      LogManager.instance()
          .log(this, Level.FINE, "beginTransaction(): FAILED db=%s user=%s err=%s", reqDb, user != null ? user :
                  "<none>",
              cause.toString(), cause);
      LogManager.instance().log(this, Level.SEVERE, "Error beginning transaction: %s", cause, cause.getMessage());
      // Pass through an already-mapped status (e.g. UNAUTHENTICATED/PERMISSION_DENIED from getDatabase)
      // instead of masking it as INTERNAL, and preserve the exception type for everything else.
      responseObserver.onError(GrpcErrorMapper.toStatusRuntimeException(cause, "Failed to begin transaction"));
    }
  }

  @Override
  public void commitTransaction(CommitTransactionRequest req, StreamObserver<CommitTransactionResponse> rsp) {
    final String txId = req.getTransaction().getTransactionId();

    LogManager.instance().log(this, Level.FINE, "commitTransaction(): received request txId=%s", txId);

    if (txId == null || txId.isBlank()) {
      LogManager.instance().log(this, Level.FINE, "commitTransaction(): missing/blank txId");
      rsp.onError(Status.INVALID_ARGUMENT.withDescription("Missing transaction id").asException());
      return;
    }

    // Peek (do NOT remove yet) so an unauthorized caller cannot evict the real owner's transaction.
    final TransactionContext txCtx = lookupActiveTransaction(txId);

    if (txCtx == null) {
      // Idempotent no-op
      LogManager.instance()
          .log(this, Level.FINE, "commitTransaction(): no active tx for id=%s, responding committed=false", txId);
      rsp.onNext(CommitTransactionResponse.newBuilder().setSuccess(true).setCommitted(false)
          .setMessage("No active transaction for id=" + txId + " (already committed/rolled back?)").build());
      rsp.onCompleted();
      return;
    }

    // Enforce ownership + per-database authorization before mutating anything. A denied caller must not be
    // able to commit (or, via the removal, disrupt) another user's transaction.
    try {
      authorizeTransactionAccess(txCtx, req.getCredentials());
    } catch (final StatusRuntimeException e) {
      rsp.onError(e);
      return;
    }

    // Now atomically claim the transaction. remove(key, value) avoids double-commit races: if a concurrent
    // commit/rollback/reaper already took it, treat this as the same idempotent no-op as an unknown id.
    if (!activeTransactions.remove(txId, txCtx)) {
      LogManager.instance()
          .log(this, Level.FINE, "commitTransaction(): tx id=%s already claimed concurrently, responding committed=false",
              txId);
      rsp.onNext(CommitTransactionResponse.newBuilder().setSuccess(true).setCommitted(false)
          .setMessage("No active transaction for id=" + txId + " (already committed/rolled back?)").build());
      rsp.onCompleted();
      return;
    }

    try {
      LogManager.instance().log(this, Level.FINE, """
          commitTransaction(): committing txId=%s on db=%s (on dedicated \
          thread)""", txId, txCtx.db.getName());

      // Execute commit ON THE SAME THREAD that began the transaction
      Future<?> commitFuture = txCtx.executor.submit(() -> {
        txCtx.db.commit();
      });
      commitFuture.get(); // Wait for commit to complete

      LogManager.instance().log(this, Level.FINE, "commitTransaction(): commit OK txId=%s", txId);
      rsp.onNext(CommitTransactionResponse.newBuilder().setSuccess(true).setCommitted(true).build());
      rsp.onCompleted();
    } catch (Throwable t) {
      Throwable cause = t instanceof ExecutionException && t.getCause() != null ? t.getCause() : t;
      LogManager.instance().log(this, Level.FINE, "commitTransaction(): commit FAILED txId=%s err=%s", txId,
          cause.toString(), cause);
      // tx is unusable; do not reinsert into the map. Map by the real cause type so a retryable
      // ConcurrentModificationException/NeedRetryException stays ABORTED while a permanent commit-time
      // DuplicatedKeyException becomes ALREADY_EXISTS (not retried forever), carrying the exception class
      // name so the client rebuilds the exact type.
      rsp.onError(GrpcErrorMapper.toStatusRuntimeException(cause, "Commit failed"));
    } finally {
      // The transaction was claimed above (removed from activeTransactions), so release its concurrency slot and
      // shut the executor down exactly once here.
      releaseTransactionSlot(txCtx.owner);
      txCtx.shutdown();
    }
  }

  @Override
  public void rollbackTransaction(RollbackTransactionRequest req, StreamObserver<RollbackTransactionResponse> rsp) {
    final String txId = req.getTransaction().getTransactionId();

    LogManager.instance().log(this, Level.FINE, "rollbackTransaction(): received request txId=%s", txId);

    if (txId == null || txId.isBlank()) {
      LogManager.instance().log(this, Level.FINE, "rollbackTransaction(): missing/blank txId");
      rsp.onError(Status.INVALID_ARGUMENT.withDescription("Missing transaction id").asException());
      return;
    }

    // Peek (do NOT remove yet) so an unauthorized caller cannot evict the real owner's transaction.
    final TransactionContext txCtx = lookupActiveTransaction(txId);

    if (txCtx == null) {
      LogManager.instance()
          .log(this, Level.FINE, "rollbackTransaction(): no active tx for id=%s, responding rolledBack=false", txId);
      rsp.onNext(RollbackTransactionResponse.newBuilder().setSuccess(true).setRolledBack(false)
          .setMessage("No active transaction for id=" + txId + " (already committed/rolled back?)").build());
      rsp.onCompleted();
      return;
    }

    // Enforce ownership + per-database authorization before mutating anything.
    try {
      authorizeTransactionAccess(txCtx, req.getCredentials());
    } catch (final StatusRuntimeException e) {
      rsp.onError(e);
      return;
    }

    // Atomically claim the transaction; if a concurrent commit/rollback/reaper already took it, treat this
    // as the same idempotent no-op as an unknown id.
    if (!activeTransactions.remove(txId, txCtx)) {
      LogManager.instance()
          .log(this, Level.FINE, "rollbackTransaction(): tx id=%s already claimed concurrently, responding rolledBack=false",
              txId);
      rsp.onNext(RollbackTransactionResponse.newBuilder().setSuccess(true).setRolledBack(false)
          .setMessage("No active transaction for id=" + txId + " (already committed/rolled back?)").build());
      rsp.onCompleted();
      return;
    }

    try {
      LogManager.instance().log(this, Level.FINE, """
          rollbackTransaction(): rolling back txId=%s on db=%s (on dedicated\
           thread)""", txId, txCtx.db.getName());

      // Execute rollback ON THE SAME THREAD that began the transaction
      Future<?> rollbackFuture = txCtx.executor.submit(() -> {
        txCtx.db.rollback();
      });
      rollbackFuture.get(); // Wait for rollback to complete

      LogManager.instance().log(this, Level.FINE, "rollbackTransaction(): rollback OK txId=%s", txId);
      rsp.onNext(RollbackTransactionResponse.newBuilder().setSuccess(true).setRolledBack(true).build());
      rsp.onCompleted();
    } catch (Throwable t) {
      Throwable cause = t instanceof ExecutionException && t.getCause() != null ? t.getCause() : t;
      LogManager.instance().log(this, Level.FINE, "rollbackTransaction(): rollback FAILED txId=%s err=%s", txId,
          cause.toString(), cause);
      rsp.onError(Status.ABORTED.withDescription("Rollback failed: " + cause.getMessage()).asException());
    } finally {
      // The transaction was claimed above (removed from activeTransactions), so release its concurrency slot and
      // shut the executor down exactly once here.
      releaseTransactionSlot(txCtx.owner);
      txCtx.shutdown();
    }
  }

  @Override
  public void streamQuery(StreamQueryRequest request, StreamObserver<QueryResult> responseObserver) {
    final QueryProfile profile = new QueryProfile();
    QueryProfile.pushCurrent(profile);
    final long engineStart = System.nanoTime();
    final AtomicBoolean cancelled = new AtomicBoolean(false);
    // Distinguishes a server-induced write timeout (we gave up on a healthy-but-slow client) from a genuine
    // client cancel: only the former should surface an explicit DEADLINE_EXCEEDED terminal to the client.
    final AtomicBoolean serverTimedOut = new AtomicBoolean(false);

    Database db = null;
    boolean beganHere = false;
    String profileLanguage = null;

    ProtocolContext.set("grpc");
    try {
      final ProjectionConfig projectionConfig = getProjectionConfigFromRequest(request);

      final ServerCallStreamObserver<QueryResult> scso = (ServerCallStreamObserver<QueryResult>) responseObserver;
      scso.setOnCancelHandler(() -> cancelled.set(true));

      final int batchSize = Math.max(1, request.getBatchSize());
      final String language = langOrDefault(request.getLanguage());
      profileLanguage = language;

      final boolean hasTx = request.hasTransaction();
      final var tx = hasTx ? request.getTransaction() : null;
      final String incomingTxId = hasTx ? tx.getTransactionId() : null;

      // If the client is inside an externally-managed transaction (started via BeginTransaction), route the
      // whole stream to that transaction's dedicated executor thread. ArcadeDB transactions are thread-bound,
      // so a stream running on the gRPC worker thread would miss the transaction's own uncommitted writes -
      // the streaming analogue of the #4260 executeQuery fix. A non-blank id that no longer resolves (e.g.
      // reaped) is rejected instead of silently running as a throwaway read.
      final TransactionContext txCtx;
      try {
        txCtx = resolveAuthorizedTransaction(incomingTxId, request.getCredentials());
      } catch (final StatusRuntimeException e) {
        responseObserver.onError(e);
        return;
      }
      if (isUnknownSuppliedTransaction(incomingTxId, txCtx)) {
        responseObserver.onError(unknownTransactionStatus(incomingTxId).asException());
        return;
      }

      if (txCtx != null) {
        db = txCtx.db;
        final Database streamDb = db;
        // The transaction lifecycle (begin/commit/rollback) stays with the Begin/Commit/Rollback RPCs, exactly
        // like executeQuery - do NOT begin or commit a throwaway read tx here.
        final Future<?> future = txCtx.executor.submit(() -> {
          dispatchStream(streamDb, request, batchSize, scso, cancelled, serverTimedOut, projectionConfig, language);
          return null;
        });
        try {
          future.get();
        } catch (final InterruptedException ie) {
          // Restore the interrupt status and surface an explicit CANCELLED terminal rather than letting the
          // outer catch mask it as a generic INTERNAL error with the interrupt flag swallowed.
          Thread.currentThread().interrupt();
          responseObserver.onError(
              Status.CANCELLED.withDescription("Stream query execution was interrupted").asRuntimeException());
          return;
        } catch (final ExecutionException ee) {
          final Throwable cause = ee.getCause() != null ? ee.getCause() : ee;
          if (cause instanceof RuntimeException re)
            throw re;
          if (cause instanceof Error err)
            throw err;
          throw new RuntimeException(cause);
        }

        if (cancelled.get()) {
          if (serverTimedOut.get()) {
            final long timeoutMs = GlobalConfiguration.SERVER_GRPC_STREAM_WRITE_TIMEOUT_MS.getValueAsLong();
            try {
              scso.onError(Status.DEADLINE_EXCEEDED
                  .withDescription("gRPC stream aborted: client transport not ready within " + timeoutMs
                      + " ms (arcadedb.server.grpcStreamWriteTimeoutMs); slow or abandoned consumer")
                  .asRuntimeException());
            } catch (final StatusRuntimeException ignore) {
              // transport may have closed concurrently; the terminal is already moot
            }
          }
          return; // terminal already sent (DEADLINE_EXCEEDED) or intentionally omitted (client cancel)
        }
        scso.onCompleted();
        return;
      }

      // No external transaction: run on the gRPC worker thread against a pooled database handle.
      db = getDatabase(request.getDatabase(), request.getCredentials());

      // --- TX begin if requested ---
      if (hasTx && tx.getBegin()) {
        db.begin();
        beganHere = true;
      }

      // --- Dispatch on mode (helpers do NOT manage transactions) ---
      dispatchStream(db, request, batchSize, scso, cancelled, serverTimedOut, projectionConfig, language);

      // If the client cancelled mid-stream, choose rollback unless caller explicitly
      // asked to commit/rollback.
      if (cancelled.get()) {
        // A server-induced write timeout is not a client cancel: the client transport is healthy, we gave up
        // on it. Surface an explicit DEADLINE_EXCEEDED first (before the best-effort rollback, so the client is
        // always signaled even if rollback fails) so it fails fast instead of blocking on its own deadline. A
        // genuine client cancel needs no terminal - its transport is already tearing down.
        if (serverTimedOut.get()) {
          final long timeoutMs = GlobalConfiguration.SERVER_GRPC_STREAM_WRITE_TIMEOUT_MS.getValueAsLong();
          try {
            scso.onError(Status.DEADLINE_EXCEEDED
                .withDescription("gRPC stream aborted: client transport not ready within " + timeoutMs
                    + " ms (arcadedb.server.grpcStreamWriteTimeoutMs); slow or abandoned consumer")
                .asRuntimeException());
          } catch (final StatusRuntimeException ignore) {
            // transport may have closed concurrently; the terminal is already moot
          }
        }
        if (hasTx) {
          if (tx.getRollback()) {
            db.rollback();
          } else if (tx.getCommit()) {
            db.commit(); // caller explicitly wanted commit even if they cancelled
          } else if (beganHere) {
            db.rollback(); // safe default on cancellation
          }
        }
        return; // terminal already sent (DEADLINE_EXCEEDED) or intentionally omitted (client cancel)
      }

      // --- TX end (normal path) — precedence: rollback > commit > begin-only ⇒
      // commit ---
      if (hasTx) {
        if (tx.getRollback()) {
          db.rollback();
        } else if (tx.getCommit()) {
          db.commit();
        } else if (beganHere) {
          db.commit();
        }
      }

      scso.onCompleted();

    } catch (Exception e) {
      // Best-effort rollback only if we began here and there wasn't an explicit
      // commit/rollback
      try {
        if (beganHere && db != null) {
          if (request.hasTransaction()) {
            var tx = request.getTransaction();
            if (!tx.getCommit() && !tx.getRollback())
              db.rollback();
          } else {
            db.rollback();
          }
        }
      } catch (Exception ignore) {
        /* no-op */
      }

      if (!cancelled.get()) {
        // Preserve an explicit gRPC status (e.g. RESOURCE_EXHAUSTED from the MATERIALIZE_ALL cap) instead of
        // masking it as INTERNAL; only genuinely unexpected failures are reported as INTERNAL.
        if (e instanceof StatusRuntimeException sre)
          responseObserver.onError(sre);
        else
          responseObserver.onError(Status.INTERNAL.withDescription("Stream query failed: " + e.getMessage()).asException());
      }
    } finally {
      // Stream endpoints mix engine iteration and row serialization throughout; expose the
      // total cost as engineNanos so the Server Profiler still captures query-level metrics.
      profile.addEngineNanos(System.nanoTime() - engineStart - profile.getEngineNanos());
      recordGrpcProfile("grpc.stream", profile, db != null ? db.getName() : request.getDatabase(),
          profileLanguage != null ? profileLanguage : request.getLanguage(), request.getQuery());
      QueryProfile.popCurrent();
      ProtocolContext.clear();
    }
  }

  /**
   * Dispatches a stream to the retrieval-mode-specific helper. The helpers do NOT manage transactions; the
   * caller owns begin/commit/rollback (or, for an externally-managed transaction, leaves the lifecycle to the
   * Begin/Commit/Rollback RPCs). PAGED mode uses SQL-specific SKIP/LIMIT wrapping, so it falls back to CURSOR
   * for non-SQL languages.
   */
  private void dispatchStream(final Database db, final StreamQueryRequest request, final int batchSize,
      final ServerCallStreamObserver<QueryResult> scso, final AtomicBoolean cancelled,
      final AtomicBoolean serverTimedOut, final ProjectionConfig projectionConfig, final String language) {
    switch (request.getRetrievalMode()) {
      case MATERIALIZE_ALL -> streamMaterialized(db, request, batchSize, scso, cancelled, serverTimedOut, projectionConfig, language);
      case PAGED -> {
        if (!"sql".equalsIgnoreCase(language))
          streamCursor(db, request, batchSize, scso, cancelled, serverTimedOut, projectionConfig, language);
        else
          streamPaged(db, request, batchSize, scso, cancelled, serverTimedOut, projectionConfig, language);
      }
      case CURSOR -> streamCursor(db, request, batchSize, scso, cancelled, serverTimedOut, projectionConfig, language);
      default -> streamCursor(db, request, batchSize, scso, cancelled, serverTimedOut, projectionConfig, language);
    }
  }

  /**
   * Mode 1 (existing behavior-ish): run once and iterate results, batching as we
   * go.
   */
  private void streamCursor(Database db, StreamQueryRequest request, int batchSize,
                            ServerCallStreamObserver<QueryResult> scso,
                            AtomicBoolean cancelled, AtomicBoolean serverTimedOut, ProjectionConfig projectionConfig, String language) {

    long running = 0L;

    QueryResult.Builder batch = QueryResult.newBuilder();
    int inBatch = 0;

    try (ResultSet rs = db.query(language, request.getQuery(),
        GrpcTypeConverter.convertParameters(request.getParametersMap()))) {

      while (rs.hasNext()) {

        if (cancelled.get())
          return;
        waitUntilReady(scso, cancelled, serverTimedOut);
        // The wait may have just flagged a server timeout (or observed a client cancel); bail out before
        // converting/emitting another row against an aborted stream.
        if (cancelled.get())
          return;

        Result r = rs.next();

        if (r.isElement()) {

          Record rec = r.getElement().get();

          batch.addRecords(convertToGrpcRecord(rec, db));

          inBatch++;
          running++;

          if (inBatch >= batchSize) {
            safeOnNext(scso, cancelled,
                batch.setTotalRecordsInBatch(inBatch).setRunningTotalEmitted(running).setIsLastBatch(false).build());
            batch = QueryResult.newBuilder();
            inBatch = 0;
          }
        } else {

          GrpcRecord.Builder rb = GrpcRecord.newBuilder();

          for (String p : r.getPropertyNames()) {
            rb.putProperties(p, convertPropToGrpcValue(p, r, projectionConfig)); // overload below
          }

          batch.addRecords(rb.build());

          inBatch++;
          running++;

          if (inBatch >= batchSize) {

            safeOnNext(scso, cancelled,
                batch.setTotalRecordsInBatch(inBatch).setRunningTotalEmitted(running).setIsLastBatch(false).build());

            batch = QueryResult.newBuilder();
            inBatch = 0;
          }
        }
      }
    }

    if (!cancelled.get() && inBatch > 0) {
      safeOnNext(scso, cancelled,
          batch.setTotalRecordsInBatch(inBatch).setRunningTotalEmitted(running).setIsLastBatch(true).build());
    }
  }

  /**
   * Mode 2: materialize everything first (simple, but can be memory-heavy).
   *
   * @param projectionConfig
   */
  private void streamMaterialized(Database db, StreamQueryRequest request, int batchSize,
                                  ServerCallStreamObserver<QueryResult> scso,
                                  AtomicBoolean cancelled, AtomicBoolean serverTimedOut, ProjectionConfig projectionConfig, String language) {

    final List<GrpcRecord> all = new ArrayList<>();

    // MATERIALIZE_ALL buffers the whole result before emitting, so bound it: a limitless query in this mode
    // would otherwise build an unbounded list and exhaust heap (DoS). Exceeding the cap fails the call with
    // RESOURCE_EXHAUSTED so the client can fall back to CURSOR/PAGED streaming.
    final int maxMaterializedRows = GlobalConfiguration.SERVER_GRPC_STREAM_MAX_MATERIALIZED_ROWS.getValueAsInteger();

    try (ResultSet rs = db.query(language, request.getQuery(),
        GrpcTypeConverter.convertParameters(request.getParametersMap()))) {

      while (rs.hasNext()) {
        if (cancelled.get())
          return;

        Result r = rs.next();

        if (r.isElement()) {

          all.add(convertToGrpcRecord(r.getElement().get(), db));
        } else {

          GrpcRecord.Builder rb = GrpcRecord.newBuilder();

          for (String p : r.getPropertyNames()) {
            rb.putProperties(p, convertPropToGrpcValue(p, r, projectionConfig)); // overload below
          }

          all.add(rb.build());
        }

        if (maxMaterializedRows > 0 && all.size() > maxMaterializedRows) {
          throw Status.RESOURCE_EXHAUSTED
              .withDescription("MATERIALIZE_ALL result exceeds the maximum of " + maxMaterializedRows
                  + " buffered rows (arcadedb.server.grpcStreamMaxMaterializedRows); use CURSOR or PAGED retrieval mode for large results")
              .asRuntimeException();
        }
      }
    }

    long running = 0L;
    for (int i = 0; i < all.size(); i += batchSize) {
      if (cancelled.get())
        return;
      waitUntilReady(scso, cancelled, serverTimedOut);
      // The wait may have just flagged a server timeout (or observed a client cancel); bail out before
      // building/emitting another batch against an aborted stream.
      if (cancelled.get())
        return;

      int end = Math.min(i + batchSize, all.size());
      QueryResult.Builder b = QueryResult.newBuilder();
      for (int j = i; j < end; j++)
        b.addRecords(all.get(j));

      running += end - i;
      safeOnNext(scso, cancelled,
          b.setTotalRecordsInBatch(end - i).setRunningTotalEmitted(running).setIsLastBatch(end == all.size()).build());
    }
  }

  /**
   * Mode 3: only fetch one page's worth of rows per emission via LIMIT/SKIP.
   *
   * @param projectionConfig
   */
  private void streamPaged(Database db, StreamQueryRequest request, int batchSize,
                           ServerCallStreamObserver<QueryResult> scso,
                           AtomicBoolean cancelled, AtomicBoolean serverTimedOut, ProjectionConfig projectionConfig, String language) {

    final String pagedSql = wrapWithSkipLimit(request.getQuery()); // see helper below
    int offset = 0;
    long running = 0L;

    while (true) {
      if (cancelled.get())
        return;
      waitUntilReady(scso, cancelled, serverTimedOut);
      // The wait may have just flagged a server timeout (or observed a client cancel); bail out before
      // running/emitting another page against an aborted stream.
      if (cancelled.get())
        return;

      Map<String, Object> params = new HashMap<>(GrpcTypeConverter.convertParameters(request.getParametersMap()));
      params.put("_skip", offset);
      params.put("_limit", batchSize);

      int count = 0;
      QueryResult.Builder b = QueryResult.newBuilder();

      try (ResultSet rs = db.query(language, pagedSql, params)) {
        while (rs.hasNext()) {
          if (cancelled.get())
            return;

          Result r = rs.next();

          if (r.isElement()) {

            b.addRecords(convertToGrpcRecord(r.getElement().get(), db));
            count++;
          } else {

            GrpcRecord.Builder rb = GrpcRecord.newBuilder();

            for (String p : r.getPropertyNames()) {
              rb.putProperties(p, convertPropToGrpcValue(p, r, projectionConfig)); // overload below
            }

            b.addRecords(rb.build());
            count++;
          }
        }
      }

      if (count == 0)
        return; // no more rows

      running += count;
      boolean last = count < batchSize;

      safeOnNext(scso, cancelled,
          b.setTotalRecordsInBatch(count).setRunningTotalEmitted(running).setIsLastBatch(last).build());

      if (last)
        return;
      offset += batchSize;
    }
  }

  private ProjectionConfig getProjectionConfigFromRequest(StreamQueryRequest request) {

    ProjectionConfig projectionConfig = null;

    if (request.hasProjectionSettings()) {

      var projectionSettings = request.getProjectionSettings();

      final boolean includeProjections = projectionSettings.getIncludeProjections();
      final var projMode = projectionSettings.getProjectionEncoding();

      int softLimit = projectionSettings.hasSoftLimitBytes() ? projectionSettings.getSoftLimitBytes().getValue() : 0; // choose your
      // default
      // or 0 = unlimited
      projectionConfig = new ProjectionConfig(includeProjections, projMode, softLimit);
    } else {

      projectionConfig = new ProjectionConfig(true, ProjectionEncoding.PROJECTION_AS_JSON, 0);
    }

    return projectionConfig;
  }

  /**
   * Wrap arbitrary SQL so we can safely inject LIMIT/SKIP outside.
   */
  private String wrapWithSkipLimit(String originalSql) {

    // Minimal defensive approach; you can do a real parser if needed.

    // ArcadeDB: SELECT FROM (...) [ORDER BY ...] SKIP :_skip LIMIT :_limit

    return "SELECT FROM (" + originalSql + ") ORDER BY @rid SKIP :_skip LIMIT :_limit";
  }

  private void safeOnNext(ServerCallStreamObserver<QueryResult> scso, AtomicBoolean cancelled, QueryResult payload) {
    if (cancelled.get())
      return;
    try {
      scso.onNext(payload);
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() == Status.Code.CANCELLED) {
        cancelled.set(true);
        return;
      }
      throw e;
    }
  }

  private void waitUntilReady(ServerCallStreamObserver<?> scso, AtomicBoolean cancelled, AtomicBoolean serverTimedOut) {
    // Honor transport readiness, but bound the wait: a slow or abandoned client must not pin this worker
    // thread (and the open ResultSet/transaction) indefinitely.
    final long timeoutMs = GlobalConfiguration.SERVER_GRPC_STREAM_WRITE_TIMEOUT_MS.getValueAsLong();
    if (!awaitTransportReady(scso::isReady, cancelled, timeoutMs)) {
      // Not ready: either the caller already cancelled, or we hit the deadline / were interrupted. In the
      // latter case mark the stream cancelled so the surrounding loop stops, rolls back, and releases the
      // ResultSet/transaction instead of busy-waiting forever, and flag it as a server-induced timeout so the
      // caller surfaces an explicit DEADLINE_EXCEEDED to the client rather than silently dropping the stream.
      if (!cancelled.get()) {
        serverTimedOut.set(true);
        cancelled.set(true);
        LogManager.instance().log(this, Level.WARNING,
            "gRPC stream aborted: client transport not ready within %d ms (slow or abandoned consumer)", timeoutMs);
      }
    }
  }

  /**
   * Waits for {@code ready} to report {@code true}, bounded by {@code timeoutMs}. Returns {@code true} only if
   * the transport became ready; returns {@code false} if {@code cancelled} is set, the deadline elapses, or the
   * thread is interrupted. A non-positive {@code timeoutMs} means wait indefinitely (legacy behavior).
   *
   * <p>Extracted from {@link #waitUntilReady} so the deadline can be unit-tested without a live gRPC transport.
   */
  static boolean awaitTransportReady(final BooleanSupplier ready, final AtomicBoolean cancelled, final long timeoutMs) {
    if (ready.getAsBoolean())
      return true;
    // Use a monotonic clock for the deadline: System.nanoTime() is immune to wall-clock adjustments (NTP/manual)
    // and the elapsed-since-start comparison avoids the overflow a large System.currentTimeMillis()+timeoutMs sum
    // could otherwise produce.
    final long startNanos = System.nanoTime();
    final long timeoutNanos = timeoutMs > 0 ? TimeUnit.MILLISECONDS.toNanos(timeoutMs) : Long.MAX_VALUE;
    while (!ready.getAsBoolean()) {
      if (cancelled.get())
        return false;
      if (timeoutMs > 0 && (System.nanoTime() - startNanos) >= timeoutNanos)
        return false;
      // avoid burning CPU:
      try {
        Thread.sleep(1);
      } catch (final InterruptedException ie) {
        Thread.currentThread().interrupt();
        return false;
      }
    }
    return true;
  }

  // --- 1) Unary bulk ---
  @Override
  public void bulkInsert(BulkInsertRequest req, StreamObserver<InsertSummary> resp) {
    final long started = System.currentTimeMillis();

    ProtocolContext.set("grpc");
    try {
      final InsertOptions opts = defaults(req.getOptions()); // apply defaults (batch size, tx mode, etc.)

      try (InsertContext ctx = new InsertContext(opts)) {

        Counts totals = insertRows(ctx, req.getRowsList().iterator());

        ctx.flushCommit(true);

        resp.onNext(ctx.summary(totals, started));
        resp.onCompleted();
      }
    } catch (Exception e) {
      resp.onError(Status.INTERNAL.withDescription("bulkInsert: " + e.getMessage()).asException());
    } finally {
      ProtocolContext.clear();
    }
  }

  // --- 2) Client-streaming; single summary at end ---
  @Override
  public StreamObserver<InsertChunk> insertStream(StreamObserver<InsertSummary> resp) {
    final ServerCallStreamObserver<InsertSummary> call = (ServerCallStreamObserver<InsertSummary>) resp;

    call.disableAutoInboundFlowControl();

    // gRPC StreamObserver is not thread-safe. Route every write/terminal call through a single
    // serialization point so a cancellation racing a terminal call cannot interleave terminal calls.
    final SynchronizedStreamObserver<InsertSummary> out = new SynchronizedStreamObserver<>(resp);

    final long startedAt = System.currentTimeMillis();

    final AtomicBoolean cancelled = new AtomicBoolean(false);
    final AtomicReference<InsertContext> ctxRef = new AtomicReference<>();

    // Issue #4806: set when a chunk fails at the transaction level (e.g. a mid-stream commit that
    // rolled the transaction back). Once set, no further rows are inserted: the stream just drains
    // the remaining inbound chunks and delivers the summary in onCompleted.
    final AtomicBoolean streamFailed = new AtomicBoolean(false);

    final Counts totals = new Counts();

    // cache the first-chunk effective options to validate consistency
    final AtomicReference<InsertOptions> firstOptsRef = new AtomicReference<>();

    call.setOnCancelHandler(() -> {
      cancelled.set(true);
      out.markTerminated();
    });

    // Pull the very first inbound message
    call.request(1);

    return new StreamObserver<>() {

      @Override
      public void onNext(InsertChunk c) {

        if (cancelled.get()) {
          return;
        }

        // Issue #4806: a previous chunk failed at the transaction level and rolled the transaction
        // back. Do not insert further rows into the broken transaction; just keep draining the
        // remaining inbound chunks so the stream still reaches onCompleted and delivers the summary.
        if (streamFailed.get()) {
          if (!cancelled.get())
            call.request(1);
          return;
        }

        try {

          InsertContext ctx = ctxRef.get();

          if (ctx == null) {

            // -------- First chunk: derive effective options and build context --------

            // Options may be provided on the first chunk; otherwise use server defaults
            InsertOptions sent = c.hasOptions() ? c.getOptions() : InsertOptions.getDefaultInstance();

            InsertOptions effective = defaults(sent); // your existing default-merging helper

            // (Optional) If your chunk carries db/creds (recommended), you can validate/set
            // them here

            // Otherwise InsertContext will resolve them via 'effective' or server defaults
            // Example if your proto carries these on the chunk:
            // String dbName = c.getDatabase();
            // DatabaseCredentials creds = c.getCredentials();
            // and pass them to the InsertContext ctor if it expects them.

            // Create and cache context
            ctx = new InsertContext(effective); // begins tx if PER_STREAM / PER_BATCH per your logic

            ctxRef.set(ctx);

            firstOptsRef.set(effective);

            LogManager.instance().log(this, Level.FINE, "insertStream: initialized context with options:\n%s",
                effective);
          } else {

            // Issue #4644: a later chunk may be dispatched on a different pool thread than the one
            // that began the transaction; re-bind the transaction to this thread before inserting.
            ctx.bindToCurrentThread();

            // -------- Subsequent chunks: optionally validate option consistency --------
            if (c.hasOptions()) {
              InsertOptions prev = firstOptsRef.get();
              InsertOptions cur = defaults(c.getOptions());
              // Minimal consistency check (customize as you wish)
              if (!sameInsertContract(prev, cur)) {
                // contract changed mid-stream -> fail fast
                throw new IllegalArgumentException("insertStream: options changed after first chunk");
              }
            }
          }

          // -------- Insert rows for this chunk --------
          Counts cts = insertRows(ctxRef.get(), c.getRowsList().iterator());
          totals.add(cts);
        } catch (Exception e) {
          // Issue #4806: per-row errors are handled row-by-row inside insertRows, so an exception
          // reaching here is a transaction-level failure (typically a mid-stream PER_BATCH/PER_ROW
          // commit that has already rolled the transaction back, or a structural error such as an
          // "options changed mid-stream" rejection in any mode). Record it once at the transaction
          // level (rowIndex -1) instead of mis-attributing it to a single row, and mark the stream
          // failed so subsequent chunks are not inserted into the broken transaction. onCompleted
          // delivers the summary. This is deliberately all-or-nothing for the failed chunk and
          // (for PER_STREAM / PER_REQUEST) for the whole stream: a structural failure is not a
          // recoverable per-row error, so the deferred commit is skipped rather than persisting a
          // partial stream the client did not intend.
          streamFailed.set(true);
          // insertRows discards its partial Counts when it throws, so the failing chunk's rows never
          // reach totals via totals.add(cts). Count them as received (the client did send them) so the
          // summary cannot report failed > received.
          //
          // Known limitation (out of scope for #4806): when server_batch_size is smaller than a single
          // chunk, insertRows may have already committed earlier mini-batches of this chunk before a
          // later mini-batch's commit failed. Those rows are durable but their inserted/updated counts
          // were in the discarded Counts, so the summary under-reports inserted for this chunk. The
          // tests use single-row chunks; a precise per-chunk reconciliation is a separate follow-up.
          totals.received += c.getRowsCount();
          totals.err(-1, commitErrorCode(e), exceptionMessage(e), "");
          // A structural failure (e.g. "options changed mid-stream") leaves the transaction still
          // active and bound to this pooled gRPC thread. Roll it back here (on the failing thread,
          // where it is bound) so its locks are released immediately and it is not leaked into a later
          // request that reuses the thread.
          final InsertContext failedCtx = ctxRef.get();
          final boolean rolledBack = failedCtx != null && failedCtx.abortTransaction();
          // If that rollback actually discarded uncommitted rows - PER_STREAM/PER_REQUEST never commit
          // before onCompleted, and PER_BATCH with a batch larger than the chunk leaves rows buffered -
          // then rows already folded into totals.inserted/updated are no longer in the database, so
          // reclassify them as failed (symmetric to recordCommitException; the -1 error is already
          // recorded above). When nothing was rolled back (rolledBack == false) the earlier batches had
          // already committed and persisted, so their counts stand.
          if (rolledBack) {
            totals.failed += totals.inserted + totals.updated;
            totals.inserted = 0;
            totals.updated = 0;
          }
        } finally {
          // Issue #5041 (TX-4): detach the transaction from this (shared) pool thread before
          // returning, so it is not left parked in the thread-local DatabaseContext between
          // callbacks where an unrelated request reusing the thread would roll it back. onNext /
          // onCompleted re-bind it via bindToCurrentThread().
          final InsertContext parkedCtx = ctxRef.get();
          if (parkedCtx != null)
            parkedCtx.unbindFromCurrentThread();
          if (!cancelled.get())
            call.request(1);
        }
      }

      @Override
      public void onError(Throwable t) {
        out.markTerminated();
        InsertContext ctx = ctxRef.get();
        if (ctx != null)
          ctx.closeQuietly();
      }

      @Override
      public void onCompleted() {

        try {

          InsertContext ctx = ctxRef.get();

          if (ctx == null) {
            // Client closed without sending a first chunk, or the first chunk failed before a context
            // could be built (Issue #4806). Deliver whatever was recorded: zeros when truly empty, or
            // the recorded error when first-chunk setup failed. Route through the thread-safe observer
            // (Issue #4801).
            out.onNext(InsertSummary.newBuilder()
                .setReceived(totals.received).setInserted(totals.inserted).setUpdated(totals.updated)
                .setIgnored(totals.ignored).setFailed(totals.failed).addAllErrors(totals.errors)
                .setExecutionTimeMs(System.currentTimeMillis() - startedAt).build());

            out.onCompleted();
            return;
          }

          // Issue #4198: PER_STREAM / PER_REQUEST defer the commit to here. A commit-time constraint
          // violation (e.g. DuplicatedKeyException) used to bubble up to the outer catch and become
          // Status.INTERNAL, leaving the client without an InsertSummary. Instead, surface those as
          // structured errors in totals.errors and still deliver the summary.
          //
          // Issue #4806: when a chunk already failed at the transaction level mid-stream the
          // transaction was rolled back, so there is nothing left to commit - attempting the deferred
          // commit would just re-raise "Transaction not begun". Skip it and deliver the summary as-is.
          if (!streamFailed.get()) {
            try {
              // Issue #4644: onCompleted may run on a different pool thread than the onNext that began
              // the transaction; re-bind it to this thread so the deferred commit finds it.
              ctx.bindToCurrentThread();
              ctx.flushCommit(true); // commit if not validate-only
            } catch (Exception commitEx) {
              recordCommitException(totals, commitEx);
            }
          }
          // Issue #4806: when streamFailed is set the onNext catch already aborted the transaction on
          // the thread it was bound to and cleared the handle, so there is nothing to clean up here.
          // Deliberately do NOT call abortTransaction()/rollback() on this (possibly different, pooled)
          // thread: it could roll back an unrelated transaction that another request left bound to it.

          if (!cancelled.get()) {
            out.onNext(ctx.summary(totals, startedAt));
            out.onCompleted();
          }
        } catch (Exception e) {
          out.onError(Status.INTERNAL.withDescription("insertStream: " + e.getMessage()).asException());
        } finally {
          InsertContext ctx = ctxRef.get();
          if (ctx != null)
            ctx.closeQuietly();
        }
      }
    };
  }

  /**
   * Issue #4198: maps a commit-time exception (DuplicatedKeyException, ValidationException, etc.) into
   * the running {@link Counts} so the client receives a structured {@link InsertSummary} instead of a
   * stream-level {@code Status.INTERNAL}. The engine auto-rolls back on commit failure, so any rows
   * that were optimistically counted as inserted/updated did not persist - reclassify them as failed.
   */
  private static void recordCommitException(final Counts totals, final Exception e) {
    final long rolledBack = totals.inserted + totals.updated;
    totals.inserted = 0;
    totals.updated = 0;
    totals.failed += rolledBack;
    if (rolledBack == 0)
      totals.failed++;

    totals.errors.add(InsertError.newBuilder()
        .setRowIndex(-1)
        .setCode(commitErrorCode(e))
        .setMessage(exceptionMessage(e))
        .setField("")
        .build());
  }

  /**
   * Classifies a transaction-level exception into the structured {@link InsertError} code:
   * {@code CONFLICT} for a {@link DuplicatedKeyException}, {@code CONTRACT_VIOLATION} for a stream
   * contract rejection ({@link IllegalArgumentException}, e.g. "options changed mid-stream", which is
   * not a commit failure at all), and {@code COMMIT_FAILED} otherwise. Shared by the mid-stream onNext
   * failure path (Issue #4806) and the deferred whole-stream commit path (Issue #4198) so the two stay
   * in sync.
   */
  private static String commitErrorCode(final Exception e) {
    if (e instanceof DuplicatedKeyException)
      return "CONFLICT";
    if (e instanceof IllegalArgumentException)
      return "CONTRACT_VIOLATION";
    return "COMMIT_FAILED";
  }

  /**
   * Returns a non-null human-readable message for an exception, falling back to the simple class name
   * when {@link Exception#getMessage()} is null.
   */
  private static String exceptionMessage(final Exception e) {
    return e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
  }

  // --- 3) Client-streaming graph batch load ---
  @Override
  public StreamObserver<GraphBatchChunk> graphBatchLoad(final StreamObserver<GraphBatchResult> resp) {
    final ServerCallStreamObserver<GraphBatchResult> call = (ServerCallStreamObserver<GraphBatchResult>) resp;
    call.disableAutoInboundFlowControl();

    // gRPC StreamObserver is not thread-safe. Route every write/terminal call through a single
    // serialization point so a cancellation racing a terminal call cannot interleave terminal calls.
    final SynchronizedStreamObserver<GraphBatchResult> out = new SynchronizedStreamObserver<>(resp);

    final long startedAt = System.currentTimeMillis();
    final AtomicBoolean cancelled = new AtomicBoolean(false);
    // errorSent gates the onCompleted flush and the call.request(1) flow-control pull; the
    // SynchronizedStreamObserver above independently guarantees terminal-call safety.
    final boolean[] errorSent = { false };
    final AtomicReference<GraphBatch> batchRef = new AtomicReference<>();
    final AtomicReference<Database> dbRef = new AtomicReference<>();
    final Map<String, RID> tempIdMap = new HashMap<>();

    // Vertex accumulation state
    final List<Object[]> vertexPropsBatch = new ArrayList<>(GRAPH_BATCH_VERTEX_BUFFER);
    final List<String> vertexTempIds = new ArrayList<>(GRAPH_BATCH_VERTEX_BUFFER);
    final String[] currentType = { null };
    final long[] counts = new long[2]; // [0]=vertices, [1]=edges
    final boolean[] inEdgePhase = { false };

    call.setOnCancelHandler(() -> {
      cancelled.set(true);
      out.markTerminated();
    });
    call.request(1);

    return new StreamObserver<>() {

      @Override
      public void onNext(final GraphBatchChunk chunk) {
        if (cancelled.get())
          return;
        try {
          GraphBatch batch = batchRef.get();
          if (batch == null) {
            // First chunk: initialize
            if (chunk.getDatabase().isEmpty())
              throw new IllegalArgumentException("First chunk must contain the database name");
            final Database db = getDatabase(chunk.getDatabase(), chunk.getCredentials());
            dbRef.set(db);
            final GraphBatch.Builder builder = db.batch();
            configureGraphBatchOptions(builder, chunk.hasOptions() ? chunk.getOptions() : null);
            batch = builder.build();
            batchRef.set(batch);
          }

          // Process records in this chunk
          for (final GraphBatchRecord rec : chunk.getRecordsList()) {
            if (rec.getKind() == GraphBatchRecord.Kind.EDGE) {
              // Flush remaining vertices on transition to edge phase
              if (!inEdgePhase[0]) {
                if (!vertexPropsBatch.isEmpty())
                  counts[0] += flushVertexBatch(batch, currentType[0], vertexPropsBatch, vertexTempIds, tempIdMap);
                inEdgePhase[0] = true;
              }
              processEdge(batch, rec, dbRef.get(), tempIdMap);
              counts[1]++;
            } else {
              if (inEdgePhase[0])
                throw new IllegalArgumentException("Vertex record received after edges. All vertices must appear before edges");

              // Type change -> flush
              final String prevType = currentType[0];
              if (prevType != null && !prevType.equals(rec.getTypeName()))
                counts[0] += flushVertexBatch(batch, prevType, vertexPropsBatch, vertexTempIds, tempIdMap);
              currentType[0] = rec.getTypeName();

              final Object[] props = rec.getPropertiesMap().isEmpty() ? new Object[0] : toPropertyArray(rec.getPropertiesMap());
              vertexPropsBatch.add(props);
              vertexTempIds.add(rec.getTempId().isEmpty() ? null : rec.getTempId());

              if (vertexPropsBatch.size() >= GRAPH_BATCH_VERTEX_BUFFER)
                counts[0] += flushVertexBatch(batch, currentType[0], vertexPropsBatch, vertexTempIds, tempIdMap);
            }
          }
        } catch (final Exception e) {
          errorSent[0] = true;
          // Null batchRef so onCompleted skips processing; skip closeQuietly to avoid blocking the
          // gRPC thread via async.waitCompletion() (buffered edges have no open transaction).
          batchRef.set(null);
          out.onError(Status.INTERNAL.withDescription("graphBatchLoad: " + e.getMessage()).asException());
          return;
        } finally {
          if (!cancelled.get() && !errorSent[0])
            call.request(1);
        }
      }

      @Override
      public void onError(final Throwable t) {
        out.markTerminated();
        closeQuietly(batchRef.getAndSet(null));
      }

      @Override
      public void onCompleted() {
        if (errorSent[0])
          return;
        try {
          final GraphBatch batch = batchRef.get();
          if (batch == null) {
            out.onNext(GraphBatchResult.newBuilder().build());
            out.onCompleted();
            return;
          }

          // Flush remaining vertices
          if (!vertexPropsBatch.isEmpty())
            counts[0] += flushVertexBatch(batch, currentType[0], vertexPropsBatch, vertexTempIds, tempIdMap);

          // Close batch -> flushes edges, connects incoming edges
          batch.close();

          final GraphBatchResult.Builder result = GraphBatchResult.newBuilder()
              .setVerticesCreated(counts[0])
              .setEdgesCreated(counts[1])
              .setElapsedMs(System.currentTimeMillis() - startedAt);

          for (final Map.Entry<String, RID> entry : tempIdMap.entrySet())
            result.putIdMapping(entry.getKey(), entry.getValue().toString());

          if (!cancelled.get()) {
            out.onNext(result.build());
            out.onCompleted();
          }
        } catch (final Exception e) {
          out.onError(Status.INTERNAL.withDescription("graphBatchLoad: " + e.getMessage()).asException());
          closeQuietly(batchRef.get());
        }
      }
    };
  }

  private Object[] toPropertyArray(final Map<String, GrpcValue> properties) {
    final Object[] result = new Object[properties.size() * 2];
    int i = 0;
    for (final Map.Entry<String, GrpcValue> entry : properties.entrySet()) {
      result[i++] = entry.getKey();
      result[i++] = GrpcTypeConverter.fromGrpcValue(entry.getValue());
    }
    return result;
  }

  private RID resolveRef(final String ref, final Database db, final Map<String, RID> tempIdMap) {
    if (ref == null || ref.isEmpty())
      throw new IllegalArgumentException("Edge record is missing from_ref or to_ref");
    if (ref.charAt(0) == '#') {
      final int colonIdx = ref.indexOf(':');
      if (colonIdx < 0)
        throw new IllegalArgumentException("Malformed RID '" + ref + "'");
      final int bucketId = Integer.parseInt(ref.substring(1, colonIdx));
      final long position = Long.parseLong(ref.substring(colonIdx + 1));
      return db.newRID(bucketId, position);
    }
    final RID rid = tempIdMap.get(ref);
    if (rid == null)
      throw new IllegalArgumentException("Unknown temporary ID '" + ref + "'. Vertices must appear before edges that reference them");
    return rid;
  }

  private int flushVertexBatch(final GraphBatch batch, final String typeName,
      final List<Object[]> propsBatch, final List<String> tempIds, final Map<String, RID> tempIdMap) {
    final int count = propsBatch.size();
    final Object[][] propsArray = propsBatch.toArray(new Object[count][]);
    final RID[] rids = batch.createVertices(typeName, propsArray);
    for (int i = 0; i < count; i++) {
      final String tempId = tempIds.get(i);
      if (tempId != null)
        tempIdMap.put(tempId, rids[i]);
    }
    propsBatch.clear();
    tempIds.clear();
    return count;
  }

  private void processEdge(final GraphBatch batch, final GraphBatchRecord rec, final Database db,
      final Map<String, RID> tempIdMap) {
    final RID srcRID = resolveRef(rec.getFromRef(), db, tempIdMap);
    final RID dstRID = resolveRef(rec.getToRef(), db, tempIdMap);
    if (rec.getPropertiesMap().isEmpty())
      batch.newEdge(srcRID, rec.getTypeName(), dstRID);
    else
      batch.newEdge(srcRID, rec.getTypeName(), dstRID, toPropertyArray(rec.getPropertiesMap()));
  }

  private void configureGraphBatchOptions(final GraphBatch.Builder builder, final GraphBatchOptions opts) {
    if (opts == null)
      return;
    if (opts.getBatchSize() > 0)
      builder.withBatchSize(opts.getBatchSize());
    if (opts.getLightEdges())
      builder.withLightEdges(true);
    if (opts.getWal())
      builder.withWAL(true);
    // Only need to disable: builder defaults to true; setting true explicitly is a no-op
    if (opts.hasParallelFlush() && !opts.getParallelFlush())
      builder.withParallelFlush(false);
    if (opts.hasPreAllocateEdgeChunks() && !opts.getPreAllocateEdgeChunks())
      builder.withPreAllocateEdgeChunks(false);
    if (opts.getEdgeListInitialSize() > 0)
      builder.withEdgeListInitialSize(opts.getEdgeListInitialSize());
    if (opts.hasBidirectional() && !opts.getBidirectional())
      builder.withBidirectional(false);
    if (opts.getCommitEvery() > 0)
      builder.withCommitEvery(opts.getCommitEvery());
    if (opts.getExpectedEdgeCount() > 0)
      builder.withExpectedEdgeCount(opts.getExpectedEdgeCount());
  }

  private void closeQuietly(final GraphBatch batch) {
    if (batch != null) {
      try { batch.close(); } catch (final Exception ignored) { }
    }
  }

  /**
   * Minimal consistency check: same "contract" for the stream.
   */
  private boolean sameInsertContract(InsertOptions a, InsertOptions b) {
    if (a == null || b == null)
      return a == b;
    // lock down the knobs that must not change mid-stream
    if (a.getTargetClass() != null && !a.getTargetClass().equals(b.getTargetClass()))
      return false;
    if (a.getConflictMode() != b.getConflictMode())
      return false;
    if (a.getTransactionMode() != b.getTransactionMode())
      return false;
    // You can also compare key/update columns if you require strict equality:
    if (!a.getKeyColumnsList().equals(b.getKeyColumnsList()))
      return false;
    if (!a.getUpdateColumnsOnConflictList().equals(b.getUpdateColumnsOnConflictList()))
      return false;
    return true;
  }

  @Override
  public StreamObserver<InsertRequest> insertBidirectional(StreamObserver<InsertResponse> resp) {
    final ServerCallStreamObserver<InsertResponse> call = (ServerCallStreamObserver<InsertResponse>) resp;
    call.disableAutoInboundFlowControl();

    // gRPC StreamObserver is not thread-safe and the single-threaded executor below dispatches
    // database work off the gRPC inbound thread. Funnel every write/terminal call through one
    // serialization point so a cancellation or duplicate terminal can never interleave on the call.
    final SynchronizedStreamObserver<InsertResponse> out = new SynchronizedStreamObserver<>(resp);

    final AtomicReference<InsertContext> ref = new AtomicReference<>();
    final AtomicBoolean cancelled = new AtomicBoolean(false);
    final AtomicBoolean started = new AtomicBoolean(false);

    // Issue #5041 (CON-1): capture the gRPC Context here, on the inbound thread where the auth
    // interceptor has attached it, so the header/Bearer-authenticated user (USER_CONTEXT_KEY) is
    // visible to resolvedUsername() on the single-thread executor below. Without this the executor
    // thread runs under Context.ROOT and header-only auth resolves to null.
    final Context grpcContext = Context.current();

    // Single-threaded executor ensures all database operations for this stream happen on the same thread.
    // This is critical because ArcadeDB transactions are ThreadLocal - if begin() and commit() happen
    // on different threads, the transaction context is lost.
    final ExecutorService streamExecutor = Executors.newSingleThreadExecutor(r -> {
      Thread t = new Thread(r, "grpc-bidi-stream-" + System.nanoTime());
      t.setDaemon(true);
      return t;
    });

    // Send responses directly - gRPC handles backpressure internally for server-side.
    // Using a queue with isReady() check doesn't work well when sending from a non-gRPC thread
    // because isReady() may return false, causing responses to be stuck.
    final Consumer<InsertResponse> sendResponse = out::onNext;

    // Cleanup helper that can be called multiple times safely
    final Runnable cleanupAndShutdown = () -> {
      cancelled.set(true);
      out.markTerminated();
      try {
        streamExecutor.submit(grpcContext.wrap(() -> {
          final InsertContext ctx = ref.getAndSet(null);
          if (ctx != null) {
            sessionWatermark.remove(ctx.sessionId);
            ctx.closeQuietly();
          }
        }));
      } catch (RejectedExecutionException ignore) {
        // Executor already shut down - cleanup was already done
      }
      streamExecutor.shutdown();
    };

    call.setOnCancelHandler(cleanupAndShutdown);

    call.request(1);

    return new StreamObserver<>() {
      @Override
      public void onNext(InsertRequest reqMsg) {
        if (cancelled.get())
          return;

        // Process every message on the single-threaded executor so the transaction ThreadLocal
        // context is preserved. Issue #5041 (CON-1): the task is wrapped with the captured gRPC
        // Context at submit time so header/Bearer auth survives the thread hop.
        final Runnable task = () -> {
          if (cancelled.get())
            return;

          try {
            switch (reqMsg.getMsgCase()) {

              case START -> {
                if (!started.compareAndSet(false, true)) {
                  out.onError(
                      Status.FAILED_PRECONDITION.withDescription("insertBidirectional: START already received").asException());
                  // Issue #5041: this is a terminal error - clean up any in-flight context (rolling
                  // back its transaction) and stop the per-stream executor so its thread does not leak.
                  final InsertContext existing = ref.getAndSet(null);
                  if (existing != null) {
                    sessionWatermark.remove(existing.sessionId);
                    existing.closeQuietly();
                  }
                  streamExecutor.shutdown();
                  return;
                }

                final InsertOptions opts = defaults(reqMsg.getStart().getOptions());
                final InsertContext ctx = new InsertContext(opts);
                ctx.startedAt = System.currentTimeMillis();
                ctx.totals = new Counts();

                ref.set(ctx);
                sessionWatermark.put(ctx.sessionId, 0L);

                sendResponse.accept(
                    InsertResponse.newBuilder().setStarted(Started.newBuilder().setSessionId(ctx.sessionId).build()).build());

                // pull next message
                call.request(1);
              }

              case CHUNK -> {
                final InsertContext ctx = require(ref.get(), "session not started");
                final InsertChunk c = reqMsg.getChunk();

                // idempotent replay guard
                final long hi = sessionWatermark.getOrDefault(ctx.sessionId, 0L);

                if (c.getChunkSeq() <= hi) {

                  out.onNext(InsertResponse.newBuilder().setBatchAck(BatchAck.newBuilder().setSessionId(ctx.sessionId)
                      .setChunkSeq(c.getChunkSeq()).setInserted(0).setUpdated(0).setIgnored(0).setFailed(0).build()).build());

                  call.request(1);
                  return;
                }

                Counts perChunk;
                try {
                  perChunk = insertRows(ctx, c.getRowsList().iterator());
                  ctx.totals.add(perChunk);
                  sessionWatermark.put(ctx.sessionId, c.getChunkSeq());

                  sendResponse.accept(InsertResponse.newBuilder()
                      .setBatchAck(BatchAck.newBuilder().setSessionId(ctx.sessionId).setChunkSeq(c.getChunkSeq())
                          .setInserted(perChunk.inserted).setUpdated(perChunk.updated).setIgnored(perChunk.ignored)
                          .setFailed(perChunk.failed).addAllErrors(perChunk.errors).build())
                      .build());

                } catch (Exception e) {
                  // surface as failed chunk and continue (or switch to resp.onError(...) if you
                  // want to fail fast)

                  perChunk = new Counts();

                  perChunk.failed = c.getRowsCount();

                  perChunk.errors.add(InsertError.newBuilder().setRowIndex(Math.max(0, ctx.received - 1)).setCode(
                          "DB_ERROR")
                      .setMessage(String.valueOf(e.getMessage())).build());
                  ctx.totals.add(perChunk);
                  // intentionally do not advance watermark on failure; client may replay safely

                  sendResponse.accept(InsertResponse.newBuilder()
                      .setBatchAck(BatchAck.newBuilder().setSessionId(ctx.sessionId).setChunkSeq(c.getChunkSeq())
                          .setInserted(perChunk.inserted).setUpdated(perChunk.updated).setIgnored(perChunk.ignored)
                          .setFailed(perChunk.failed).addAllErrors(perChunk.errors).build())
                      .build());
                }

                call.request(1);
              }

              case COMMIT -> {
                final InsertContext ctx = require(ref.get(), "session not started");
                try {
                  ctx.flushCommit(true); // commit unless validate_only in your InsertContext logic
                  final InsertSummary sum = ctx.summary(ctx.totals, ctx.startedAt);
                  out.onNext(InsertResponse.newBuilder().setCommitted(Committed.newBuilder().setSummary(sum).build()).build());
                  out.onCompleted();
                } catch (Exception e) {
                  out.onError(Status.INTERNAL.withDescription("commit: " + e.getMessage()).asException());
                } finally {
                  sessionWatermark.remove(ctx.sessionId);
                  ctx.closeQuietly();
                  ref.set(null);
                  // Issue #5041: COMMIT (success or failure) is terminal - stop the per-stream
                  // executor so its thread does not leak when the client does not half-close.
                  streamExecutor.shutdown();
                }
              }

              case MSG_NOT_SET -> {
                // ignore
                call.request(1);
              }
            }
          } catch (Exception unexpected) {
            // defensive: fail fast on unexpected exceptions
            out.onError(Status.INTERNAL.withDescription("insertBidirectional: " + unexpected.getMessage()).asException());
            final InsertContext ctx = ref.getAndSet(null);
            if (ctx != null) {
              sessionWatermark.remove(ctx.sessionId);
              ctx.closeQuietly();
            }
            // Issue #5041: an unexpected failure is terminal - stop the per-stream executor so its
            // thread does not leak after the error is delivered to the client.
            streamExecutor.shutdown();
          }
        };

        // Issue #5041 (CON-4): guard the submit against a RejectedExecutionException thrown when a
        // racing cancel shut the executor down between the cancelled check above and this submit
        // (onCompleted already guards this).
        try {
          streamExecutor.submit(grpcContext.wrap(task));
        } catch (final RejectedExecutionException ignore) {
          // a racing cancel shut the executor down; drop the message instead of throwing out of the callback
        }
      }

      @Override
      public void onError(Throwable t) {
        cleanupAndShutdown.run();
      }

      @Override
      public void onCompleted() {
        // Issue #5041 (TX-6): half-close WITHOUT an explicit COMMIT must roll back. The previous
        // flushCommit(false) actually COMMITTED the buffered rows for PER_ROW/PER_BATCH
        // (db.commit();db.begin()) and left the open transaction leaked for PER_STREAM, contradicting
        // the "commit only on explicit COMMIT" contract. closeQuietly() -> close() rebinds and rolls
        // back the still-open transaction (a no-op if a prior per-batch commit already terminated it)
        // for every mode, then drops the thread-local context - a single rollback path.
        try {
          streamExecutor.submit(grpcContext.wrap(() -> {
            final InsertContext ctx = ref.getAndSet(null);
            if (ctx != null) {
              sessionWatermark.remove(ctx.sessionId);
              ctx.closeQuietly();
            }
          }));
        } catch (RejectedExecutionException ignore) {
          // Executor already shut down
        }
        streamExecutor.shutdown();
      }
    };
  }

  private Object recordValue(final GrpcRecord r, final String col) {
    final GrpcValue v = r.getPropertiesMap().get(col);
    return v == null ? null : fromGrpcValue(v);
  }

  // Merge incoming values onto the matched record (explicit update_columns, else all non-key props).
  // Skips @-fields and absent/null columns (so a field cannot be nulled here), and edge out/in -
  // writing those via set() would bypass the graph engine's vertex edge-list bookkeeping.
  private void applyConflictUpdates(final InsertContext ctx, final GrpcRecord r, final boolean isEdge,
      final MutableDocument existing) {
    final boolean mergeAll = ctx.updateCols.isEmpty();
    final Iterable<String> cols = mergeAll ? r.getPropertiesMap().keySet() : ctx.updateCols;
    for (final String col : cols) {
      if (col.startsWith("@"))
        continue;
      if (mergeAll && ctx.keyColsSet.contains(col))
        continue;
      if (isEdge && ("out".equals(col) || "in".equals(col)))
        continue;
      final Object value = recordValue(r, col);
      if (value == null)
        continue;
      existing.set(col, value);
    }
  }

  // Backtick-quote a client-supplied identifier (target class / key column) so it cannot inject SQL;
  // values stay parameterized via '?'. Embedded backticks are escaped per the SQL grammar, which
  // also requires at least one character between the backticks.
  private static String quoteName(final String name) {
    if (name == null || name.isEmpty())
      throw new IllegalArgumentException("SQL identifier must not be empty");
    return "`" + name.replace("`", "\\`") + "`";
  }

  // Match an existing row by key and merge onto it; false when none matched (caller then inserts).
  // MutableVertex and MutableEdge both extend MutableDocument, so asDocument().modify() returns the
  // record's real mutable subtype; save() therefore persists a vertex/edge through its own path.
  private boolean tryUpsertByRecord(final InsertContext ctx, final GrpcRecord r, final boolean isEdge) {
    final List<String> keys = ctx.keyCols;
    if (keys.isEmpty())
      return false;

    final String where = String.join(" AND ", keys.stream().map(k -> quoteName(k) + " = ?").toList());
    final Object[] params = keys.stream().map(k -> recordValue(r, k)).toArray();

    try (final ResultSet rs = ctx.db.query("sql", "SELECT FROM " + quoteName(ctx.opts.getTargetClass()) + " WHERE " + where, params)) {
      if (!rs.hasNext())
        return false;

      final Result res = rs.next();
      if (!res.isElement())
        return false;

      final MutableDocument existing = res.getElement().get().asDocument().modify();
      applyConflictUpdates(ctx, r, isEdge, existing);
      existing.save();
      return true;
    }
  }

  private boolean keyExistsByRecord(final InsertContext ctx, final GrpcRecord r) {
    if (ctx.keyCols.isEmpty())
      return false;
    final String where = String.join(" AND ", ctx.keyCols.stream().map(k -> quoteName(k) + " = ?").toList());
    final Object[] params = ctx.keyCols.stream().map(k -> recordValue(r, k)).toArray();
    try (final ResultSet rs = ctx.db.query("sql", "SELECT FROM " + quoteName(ctx.opts.getTargetClass()) + " WHERE " + where, params)) {
      return rs.hasNext();
    }
  }

  // ---------- Core insert plumbing ----------

  private static final class Counts {

    long received, inserted, updated, ignored, failed;

    final List<InsertError> errors = new ArrayList<>();

    void add(Counts o) {
      received += o.received;
      inserted += o.inserted;
      updated += o.updated;
      ignored += o.ignored;
      failed += o.failed;
      errors.addAll(o.errors);
    }

    void err(long rowIndex, String code, String msg, String field) {
      failed++;
      errors.add(InsertError.newBuilder().setRowIndex(rowIndex).setCode(code).setMessage(msg).setField(field).build());
    }
  }

  private Counts insertRows(InsertContext ctx, Iterator<GrpcRecord> it) {

    Counts c = new Counts();

    int inBatch = 0;

    Schema schema = ctx.db.getSchema();
    DocumentType dt = schema.getType(ctx.opts.getTargetClass());
    final boolean isVertex = dt instanceof VertexType;
    final boolean isEdge = dt instanceof EdgeType;

    // Tell callers their out/in update columns are being ignored, rather than dropping them silently.
    if (isEdge && !ctx.warnedEdgeEndpointUpdateCols && (ctx.updateCols.contains("out") || ctx.updateCols.contains("in"))) {
      ctx.warnedEdgeEndpointUpdateCols = true;
      LogManager.instance().log(this, Level.WARNING,
          "InsertStream upsert on edge type '%s': 'out'/'in' in update_columns_on_conflict are ignored (edge endpoints cannot be re-pointed via upsert)",
          ctx.opts.getTargetClass());
    }

    // Reject blank key columns with a clear client-visible error instead of letting the per-row
    // identifier quoting throw a generic DB_ERROR for every record.
    for (final String kc : ctx.keyCols)
      if (kc == null || kc.isBlank()) {
        c.err(-1, "INVALID_KEY_COLUMN", "key_columns must not contain empty names", "");
        return c;
      }

    while (it.hasNext()) {

      GrpcRecord r = it.next();

      c.received++;
      ctx.received++;

      try {
        if (ctx.opts.getValidateOnly())
          continue;

        final ConflictMode mode = ctx.opts.getConflictMode();

        if (isVertex) {
          if (mode == ConflictMode.CONFLICT_UPDATE && tryUpsertByRecord(ctx, r, false)) {
            c.updated++;
          } else if (mode == ConflictMode.CONFLICT_IGNORE && keyExistsByRecord(ctx, r)) {
            c.ignored++;
          } else {
            final MutableVertex v = ctx.db.newVertex(ctx.opts.getTargetClass());
            applyGrpcRecord(v, r);
            v.save();
            c.inserted++;
          }
        } else if (isEdge) {
          // Edges must honor conflict_mode / key_columns like documents and vertices. newEdge()
          // persists and links the edge immediately, so the upsert/ignore check has to run before the
          // edge is created, otherwise a match would leave a dangling edge.
          if (mode == ConflictMode.CONFLICT_UPDATE && tryUpsertByRecord(ctx, r, true)) {
            c.updated++;
          } else if (mode == ConflictMode.CONFLICT_IGNORE && keyExistsByRecord(ctx, r)) {
            c.ignored++;
          } else {
            final String outRid = getStringProp(r, "out");
            final String inRid = getStringProp(r, "in");

            if (outRid == null || inRid == null) {

              c.failed++;

              c.errors.add(InsertError.newBuilder().setRowIndex(ctx.received - 1).setCode("MISSING_ENDPOINTS")
                  .setMessage("Edge requires 'out' and 'in'").build());
            } else {
              final var outV = ctx.db.lookupByRID(new RID(outRid), false).asVertex(false);

              // Create edge from the OUT vertex. The `in` RID is stored in the edge record; use DatabaseRID so edge.getIn() resolves across threads.
              final MutableEdge e = outV.newEdge(ctx.opts.getTargetClass(), ctx.db.newRID(inRid));
              applyGrpcRecord(e, r); // sets edge properties
              e.save();
              c.inserted++;
            }
          }
        } else {
          if (mode == ConflictMode.CONFLICT_UPDATE && tryUpsertByRecord(ctx, r, false)) {
            c.updated++;
          } else if (mode == ConflictMode.CONFLICT_IGNORE && keyExistsByRecord(ctx, r)) {
            c.ignored++;
          } else {
            final MutableDocument d = ctx.db.newDocument(ctx.opts.getTargetClass());
            applyGrpcRecord(d, r);
            d.save();
            c.inserted++;
          }
        }

      } catch (DuplicatedKeyException dup) {
        switch (ctx.opts.getConflictMode()) {
          case CONFLICT_IGNORE -> c.ignored++;
          case CONFLICT_ABORT, UNRECOGNIZED -> c.err(ctx.received - 1, "CONFLICT", dup.getMessage(), "");
          // A concurrent stream inserted this key after our check; the unique index proves it exists
          // now, so retry as an update instead of losing the row. Not exercised by
          // Issue4656InsertStreamConflictUpdateIT: the race needs two concurrent streams hitting the
          // same new key, which is not deterministically reproducible single-threaded.
          case CONFLICT_UPDATE -> {
            try {
              if (tryUpsertByRecord(ctx, r, isEdge))
                c.updated++;
              else
                // The match vanished between the conflict and the retry (transient MVCC window): report
                // it as a retriable CONFLICT rather than guessing.
                c.err(ctx.received - 1, "CONFLICT", dup.getMessage(), "");
            } catch (DuplicatedKeyException retryDup) {
              // A third writer can race the retry too: still a retriable conflict.
              c.err(ctx.received - 1, "CONFLICT", retryDup.getMessage(), "");
            } catch (Exception retryEx) {
              // Anything else (IO error, etc.) is a real failure - do not mask it as a CONFLICT.
              c.err(ctx.received - 1, "DB_ERROR", retryEx.getMessage(), "");
            }
          }
          case CONFLICT_ERROR -> c.err(ctx.received - 1, "CONFLICT", dup.getMessage(), "");
        }
      } catch (Exception e) {
        c.err(ctx.received - 1, "DB_ERROR", e.getMessage(), "");
      }

      inBatch++;
      if (ctx.opts.getTransactionMode() == TransactionMode.PER_BATCH && inBatch >= serverBatchSize(ctx)) {
        ctx.flushCommit(false);
        inBatch = 0;
      } else if (ctx.opts.getTransactionMode() == TransactionMode.PER_ROW) {
        ctx.flushCommit(false);
        inBatch = 0;
      }
    }
    return c;
  }

  private String getStringProp(GrpcRecord r, String key) {
    if (r == null || key == null || key.isEmpty())
      return null;
    GrpcValue v = r.getPropertiesMap().get(key);
    if (v == null)
      return null;
    if (v.getKindCase() == GrpcValue.KindCase.STRING_VALUE)
      return v.getStringValue();
    return String.valueOf(fromGrpcValue(v));
  }

  private int serverBatchSize(InsertContext ctx) {
    return ctx.opts.getServerBatchSize() == 0 ? 1000 : ctx.opts.getServerBatchSize();
  }

  private void applyGrpcRecord(MutableDocument doc, GrpcRecord r) {
    r.getPropertiesMap().forEach((k, grpcVal) -> {
      // Skip system fields
      if (k.startsWith("@"))
        return;
      Object javaVal = fromGrpcValue(grpcVal);
      if (LogManager.instance().isDebugEnabled())
        LogManager.instance().log(this, Level.FINE, "APPLY-DOC %s <= %s -> %s", k, summarizeGrpc(grpcVal),
            summarizeJava(javaVal));
      doc.set(k, javaVal);
    });
  }

  private void applyGrpcRecord(MutableVertex vertex, GrpcRecord r) {
    r.getPropertiesMap().forEach((k, grpcVal) -> {
      // Skip system fields
      if (k.startsWith("@"))
        return;
      Object javaVal = fromGrpcValue(grpcVal);
      if (LogManager.instance().isDebugEnabled())
        LogManager.instance()
            .log(this, Level.FINE, "APPLY-VERTEX %s <= %s -> %s", k, summarizeGrpc(grpcVal), summarizeJava(javaVal));
      vertex.set(k, javaVal);
    });
  }

  private void applyGrpcRecord(MutableEdge edge, GrpcRecord r) {
    r.getPropertiesMap().forEach((k, grpcVal) -> {
      // Skip system fields
      if (k.startsWith("@"))
        return;
      Object javaVal = fromGrpcValue(grpcVal);
      if (LogManager.instance().isDebugEnabled())
        LogManager.instance().log(this, Level.FINE, "APPLY-EDGE %s <= %s -> %s", k, summarizeGrpc(grpcVal),
            summarizeJava(javaVal));
      edge.set(k, javaVal);
    });
  }

  private Object fromGrpcValue(GrpcValue v) {
    if (v == null)
      return dbgDec("fromGrpcValue", v, null, null);
    switch (v.getKindCase()) {
      case BOOL_VALUE:
        return dbgDec("fromGrpcValue", v, v.getBoolValue(), null);
      case INT32_VALUE:
        return dbgDec("fromGrpcValue", v, v.getInt32Value(), null);
      case INT64_VALUE:
        return dbgDec("fromGrpcValue", v, v.getInt64Value(), null);
      case FLOAT_VALUE:
        return dbgDec("fromGrpcValue", v, v.getFloatValue(), null);
      case DOUBLE_VALUE:
        return dbgDec("fromGrpcValue", v, v.getDoubleValue(), null);
      case STRING_VALUE:
        return dbgDec("fromGrpcValue", v, v.getStringValue(), null);
      case BYTES_VALUE:
        return dbgDec("fromGrpcValue", v, v.getBytesValue().toByteArray(), null);
      case TIMESTAMP_VALUE: {
        // Issue #4149: keep nanosecond precision through parameter binding so DATETIME_MICROS /
        // DATETIME_NANOS columns receive the precision the proto Timestamp natively carries.
        final Timestamp ts = v.getTimestampValue();
        return dbgDec("fromGrpcValue", v, Instant.ofEpochSecond(ts.getSeconds(), ts.getNanos()), null);
      }
      case LINK_VALUE:
        return dbgDec("fromGrpcValue", v, new RID(v.getLinkValue().getRid()), null);

      case DECIMAL_VALUE: {
        var d = v.getDecimalValue();
        return dbgDec("fromGrpcValue", v, new BigDecimal(BigInteger.valueOf(d.getUnscaled()), d.getScale()),
            null);
      }

      case LIST_VALUE: {
        var out = new ArrayList<>();
        for (GrpcValue e : v.getListValue().getValuesList())
          out.add(fromGrpcValue(e));
        return dbgDec("fromGrpcValue", v, out, null);
      }

      case MAP_VALUE: {
        var out = new LinkedHashMap<String, Object>();
        v.getMapValue().getEntriesMap().forEach((k, vv) -> out.put(k, fromGrpcValue(vv)));
        return dbgDec("fromGrpcValue", v, out, null);
      }

      case EMBEDDED_VALUE: {
        var out = new LinkedHashMap<String, Object>();
        v.getEmbeddedValue().getFieldsMap().forEach((k, vv) -> out.put(k, fromGrpcValue(vv)));
        return dbgDec("fromGrpcValue", v, out, null);
      }

      case KIND_NOT_SET:
        return dbgDec("fromGrpcValue", v, null, null);
    }
    return dbgDec("fromGrpcValue", v, null, null);
  }

  private GrpcValue toGrpcValue(Object o) {

    return toGrpcValue(o, null);
  }

  private GrpcValue toGrpcValue(Object o, ProjectionConfig pc) {

    LogManager.instance().log(this, Level.FINE, "toGrpcValue(): Converting\n   value = %s\n   class = %s", o,
        o == null ? "null" : o.getClass().getName());

    if (o instanceof JsonElement je) {
      return gsonToGrpc(je);
    }

    GrpcValue.Builder b = GrpcValue.newBuilder();

    if (o == null)
      return dbgEnc("toGrpcValue", o, b.build(), null);

    if (o instanceof Boolean v)
      return dbgEnc("toGrpcValue", o, b.setBoolValue(v).build(), null);
    if (o instanceof Integer v)
      return dbgEnc("toGrpcValue", o, b.setInt32Value(v).build(), null);
    if (o instanceof Long v)
      return dbgEnc("toGrpcValue", o, b.setInt64Value(v).build(), null);
    if (o instanceof Float v)
      return dbgEnc("toGrpcValue", o, b.setFloatValue(v).build(), null);
    if (o instanceof Double v)
      return dbgEnc("toGrpcValue", o, b.setDoubleValue(v).build(), null);
    if (o instanceof CharSequence v)
      return dbgEnc("toGrpcValue", o, b.setStringValue(v.toString()).build(), null);
    if (o instanceof byte[] v)
      return dbgEnc("toGrpcValue", o, b.setBytesValue(ByteString.copyFrom(v)).build(), null);

    if (o instanceof Date v) {
      return dbgEnc("toGrpcValue", o,
          GrpcValue.newBuilder().setTimestampValue(GrpcTypeConverter.msToTimestamp(v.getTime())).setLogicalType(
              "datetime").build(), null);
    }

    // Issue #4149: emit Java time temporals as proto Timestamp instead of falling through to
    // String.valueOf(o), which silently dropped sub-millisecond precision.
    if (o instanceof LocalDate ld) {
      final long seconds = ld.atStartOfDay(ZoneOffset.UTC).toEpochSecond();
      return dbgEnc("toGrpcValue", o, GrpcValue.newBuilder()
          .setTimestampValue(Timestamp.newBuilder().setSeconds(seconds).setNanos(0).build())
          .setLogicalType("date").build(), null);
    }
    if (o instanceof LocalDateTime ldt) {
      final Instant instant = ldt.toInstant(ZoneOffset.UTC);
      return dbgEnc("toGrpcValue", o, GrpcValue.newBuilder()
          .setTimestampValue(Timestamp.newBuilder().setSeconds(instant.getEpochSecond()).setNanos(instant.getNano()).build())
          .setLogicalType("datetime").build(), null);
    }
    if (o instanceof Instant instant) {
      return dbgEnc("toGrpcValue", o, GrpcValue.newBuilder()
          .setTimestampValue(Timestamp.newBuilder().setSeconds(instant.getEpochSecond()).setNanos(instant.getNano()).build())
          .setLogicalType("datetime").build(), null);
    }
    if (o instanceof ZonedDateTime zdt) {
      final Instant instant = zdt.toInstant();
      return dbgEnc("toGrpcValue", o, GrpcValue.newBuilder()
          .setTimestampValue(Timestamp.newBuilder().setSeconds(instant.getEpochSecond()).setNanos(instant.getNano()).build())
          .setLogicalType("datetime").build(), null);
    }

    if (o instanceof RID rid) {
      return dbgEnc("toGrpcValue", o,
          GrpcValue.newBuilder().setLinkValue(GrpcLink.newBuilder().setRid(rid.toString()).build()).setLogicalType(
              "rid").build(),
          null);
    }

    if (o instanceof BigDecimal v) {
      var unscaled = v.unscaledValue();
      if (unscaled.bitLength() <= 63) {
        return GrpcValue.newBuilder()
            .setDecimalValue(GrpcDecimal.newBuilder().setUnscaled(unscaled.longValue()).setScale(v.scale()))
            .setLogicalType("decimal").build();
      } else {
        // if you need >64-bit unscaled, switch GrpcDecimal.unscaled to bytes in the
        // proto
        return dbgEnc("toGrpcValue", o, GrpcValue.newBuilder().setStringValue(v.toPlainString()).setLogicalType(
                "decimal").build(),
            null);
      }
    }

    if (o instanceof Document edoc && edoc.getIdentity() == null) {
      GrpcEmbedded.Builder eb = GrpcEmbedded.newBuilder();
      if (edoc.getType() != null)
        eb.setType(edoc.getTypeName());
      for (String k : edoc.getPropertyNames()) {
        eb.putFields(k, toGrpcValue(edoc.get(k)));
      }
      return dbgEnc("toGrpcValue", o, GrpcValue.newBuilder().setEmbeddedValue(eb.build()).build(), null);
    }

    // ===== PROJECTION-AWARE Document handling =====
    if (o instanceof Document doc) {

      final boolean inProjection = pc != null && pc.include;

      if (!inProjection) {
        // Not a projection row: send as LINK if possible, else fall back
        if (doc.getIdentity() != null && doc.getIdentity().isValid()) {
          LogManager.instance()
              .log(this, Level.FINE, "GRPC-ENC [toGrpcValue] DOC-NON-PROJECTION -> LINK rid=%s", doc.getIdentity());
          return GrpcValue.newBuilder().setLinkValue(GrpcLink.newBuilder().setRid(doc.getIdentity().toString()).build())
              .setLogicalType("rid").build();
        }
        // No identity → treat as EMBEDDED-like MAP
        LogManager.instance().log(this, Level.FINE, "GRPC-ENC [toGrpcValue] DOC-NON-PROJECTION (no rid) -> MAP");
        GrpcMap.Builder mb = GrpcMap.newBuilder();
        if (doc.getType() != null)
          mb.putEntries("@type", GrpcValue.newBuilder().setStringValue(doc.getTypeName()).build());
        for (String k : doc.getPropertyNames()) {
          mb.putEntries(k, toGrpcValue(doc.get(k), pc));
        }
        return GrpcValue.newBuilder().setMapValue(mb.build()).build();
      }

      // In a PROJECTION row: obey encoding mode
      ProjectionEncoding enc = pc.enc;

      // 3.1 LINK mode
      if (enc == ProjectionEncoding.PROJECTION_AS_LINK) {
        if (doc.getIdentity() != null && doc.getIdentity().isValid()) {
          LogManager.instance().log(this, Level.FINE, "GRPC-ENC [toGrpcValue] PROJECTION -> LINK rid=%s",
              doc.getIdentity());
          return GrpcValue.newBuilder().setLinkValue(GrpcLink.newBuilder().setRid(doc.getIdentity().toString()).build())
              .setLogicalType("rid").build();
        }
        // No rid: fall back to MAP
        LogManager.instance().log(this, Level.FINE, "GRPC-ENC [toGrpcValue] PROJECTION LINK fallback -> MAP (no rid)");
        enc = ProjectionEncoding.PROJECTION_AS_MAP;
      }

      // 3.2 MAP mode
      if (enc == ProjectionEncoding.PROJECTION_AS_MAP) {
        LogManager.instance().log(this, Level.FINE, "GRPC-ENC [toGrpcValue] PROJECTION -> MAP rid=%s type=%s",
            doc.getIdentity() != null ? doc.getIdentity() : "null", doc.getType() != null ? doc.getTypeName() :
                "null");
        GrpcMap.Builder mb = GrpcMap.newBuilder();

        // meta
        if (doc.getIdentity() != null && doc.getIdentity().isValid()) {
          GrpcValue ridVal =
              GrpcValue.newBuilder().setLinkValue(GrpcLink.newBuilder().setRid(doc.getIdentity().toString()).build())
                  .setLogicalType("rid").build();
          // soft limit accounting (precise, using serialized sizes)
          if (!pc.wouldExceed(GrpcTypeConverter.bytesOf("@rid") + ridVal.getSerializedSize())) {
            mb.putEntries("@rid", ridVal);
          } else {
            pc.truncated = true;
          }
        }
        if (doc.getType() != null) {
          GrpcValue typeVal = GrpcValue.newBuilder().setStringValue(doc.getTypeName()).build();
          if (!pc.wouldExceed(GrpcTypeConverter.bytesOf("@type") + typeVal.getSerializedSize())) {
            mb.putEntries("@type", typeVal);
          } else {
            pc.truncated = true;
          }
        }

        for (String k : doc.getPropertyNames()) {
          GrpcValue child = toGrpcValue(doc.get(k), pc); // recurse with config
          int add = GrpcTypeConverter.bytesOf(k) + child.getSerializedSize();
          if (pc.wouldExceed(add)) {
            LogManager.instance()
                .log(this, Level.FINE, """
                        GRPC-ENC [toGrpcValue] PROJECTION MAP soft-limit hit; skipping '%s' \
                        (limit=%s, used~%s)""",
                    k,
                    pc.softLimitBytes, pc.used.get());
            pc.truncated = true;
            break;
          }
          mb.putEntries(k, child);
        }

        return GrpcValue.newBuilder().setMapValue(mb.build()).build();
      }

      // 3.3 JSON mode
      if (enc == ProjectionEncoding.PROJECTION_AS_JSON) {
        LogManager.instance().log(this, Level.FINE, "GRPC-ENC [toGrpcValue] PROJECTION -> JSON rid=%s type=%s",
            doc.getIdentity() != null ? doc.getIdentity() : "null", doc.getType() != null ? doc.getTypeName() :
                "null");

        try {

          // Let ArcadeDB serialize the document/result properly

          boolean hasEmptyCollection = false;
          for (String k : doc.getPropertyNames()) {
            Object v = doc.get(k);
            if (v instanceof Collection<?> c && c.isEmpty()) {
              hasEmptyCollection = true;
              break;
            }
          }

          var json = (hasEmptyCollection ? SAFE : FAST).serializeDocument(doc);

          byte[] jsonBytes = json.toString().getBytes(StandardCharsets.UTF_8);

          // Soft limit handling
          if (pc.softLimitBytes > 0 && jsonBytes.length > pc.softLimitBytes) {
            pc.truncated = true;
            LogManager.instance().log(this, Level.FINE, """
                    GRPC-ENC [toGrpcValue] PROJECTION JSON soft-limit hit; \
                    size=%s limit=%s""",
                jsonBytes.length,
                pc.softLimitBytes);
            // Prefer a RID fallback if we have one
            if (doc.getIdentity() != null && doc.getIdentity().isValid()) {
              return GrpcValue.newBuilder().setLinkValue(GrpcLink.newBuilder().setRid(doc.getIdentity().toString()).build())
                  .setLogicalType("rid").build();
            } else {
              jsonBytes = "{\"__truncated\":true}".getBytes(StandardCharsets.UTF_8);
            }
          } else {
            pc.used.addAndGet(jsonBytes.length);
          }

          return GrpcValue.newBuilder().setBytesValue(ByteString.copyFrom(jsonBytes)).setLogicalType("json")
              .build();
        } catch (Throwable t) {
          // Safe fallback: send as MAP (typed GrpcValue tree)
          LogManager.instance().log(this, Level.WARNING,
              "GRPC-ENC [toGrpcValue] PROJECTION_AS_JSON failed for rid=%s type=%s, falling back to MAP; err=%s",
              doc.getIdentity() != null ? doc.getIdentity() : "null", doc.getType() != null ? doc.getTypeName() :
                  "null",
              t.toString());

          GrpcMap.Builder mb = GrpcMap.newBuilder();

          if (doc.getIdentity() != null && doc.getIdentity().isValid()) {
            mb.putEntries("@rid", GrpcValue.newBuilder()
                .setLinkValue(GrpcLink.newBuilder().setRid(doc.getIdentity().toString()).build()).setLogicalType("rid"
                ).build());
          }
          if (doc.getType() != null) {
            mb.putEntries("@type", GrpcValue.newBuilder().setStringValue(doc.getTypeName()).build());
          }
          for (String k : doc.getPropertyNames()) {
            mb.putEntries(k, toGrpcValue(doc.get(k)));
          }

          return GrpcValue.newBuilder().setMapValue(mb.build()).build();
        }
      }

      // Shouldn't get here, but fall back
      LogManager.instance()
          .log(this, Level.FINE, "GRPC-ENC [toGrpcValue] PROJECTION unknown encoding %s; falling back to LINK/STRING",
              enc.name());
      if (doc.getIdentity() != null && doc.getIdentity().isValid()) {
        return GrpcValue.newBuilder().setLinkValue(GrpcLink.newBuilder().setRid(doc.getIdentity().toString()).build())
            .setLogicalType("rid").build();
      }

      // else small MAP fallback
      GrpcMap.Builder mb = GrpcMap.newBuilder();
      for (String k : doc.getPropertyNames())
        mb.putEntries(k, toGrpcValue(doc.get(k), pc));
      return GrpcValue.newBuilder().setMapValue(mb.build()).build();
    }

    if (o instanceof Map<?, ?> m) {
      GrpcMap.Builder mb = GrpcMap.newBuilder();
      for (var e : m.entrySet()) {
        mb.putEntries(String.valueOf(e.getKey()), toGrpcValue(e.getValue()));
      }
      return dbgEnc("toGrpcValue", o, GrpcValue.newBuilder().setMapValue(mb.build()).build(), null);
    }

    if (o instanceof Collection<?> c) {
      GrpcList.Builder lb = GrpcList.newBuilder();
      for (Object e : c)
        lb.addValues(toGrpcValue(e));
      return dbgEnc("toGrpcValue", o, GrpcValue.newBuilder().setListValue(lb.build()).build(), null);
    }

    if (o.getClass().isArray()) {
      int len = java.lang.reflect.Array.getLength(o);
      GrpcList.Builder lb = GrpcList.newBuilder();
      for (int i = 0; i < len; i++) {
        lb.addValues(toGrpcValue(java.lang.reflect.Array.get(o, i)));
      }
      return dbgEnc("toGrpcValue", o, GrpcValue.newBuilder().setListValue(lb.build()).build(), null);
    }

    // RID/Identifiable string fallback
    if (o instanceof Identifiable id)
      return dbgEnc("toGrpcValue", o, GrpcValue.newBuilder()
          .setLinkValue(GrpcLink.newBuilder().setRid(id.getIdentity().toString()).build()).setLogicalType("rid").build(), null);

    // Support ArcadeDB Result/ResultInternal as structural MAP
    if (o instanceof Result res) {
      GrpcMap.Builder mb = GrpcMap.newBuilder();
      try {
        for (String k : res.getPropertyNames()) {
          mb.putEntries(k, toGrpcValue(res.getProperty(k)));
        }
      } catch (Throwable ignore) {
      }

      return dbgEnc("toGrpcValue", o, GrpcValue.newBuilder().setMapValue(mb.build()).build(), null);
    }

    // Fallback
    LogManager.instance()
        .log(this, Level.FINE, "GRPC-ENC [toGrpcValue] FALLBACK-TO-STRING for class=%s value=%s",
            o.getClass().getName(),
            String.valueOf(o));

    return dbgEnc("toGrpcValue", o, GrpcValue.newBuilder().setStringValue(String.valueOf(o)).build(), null);
  }

  private InsertOptions defaults(InsertOptions in) {
    InsertOptions.Builder b = in.toBuilder();
    if (in.getServerBatchSize() == 0)
      b.setServerBatchSize(1000);
    if (in.getTransactionMode() == TransactionMode.UNRECOGNIZED)
      b.setTransactionMode(TransactionMode.PER_BATCH);
    return b.build();
  }

  private static <T> T require(T v, String msg) {
    if (v == null)
      throw Status.FAILED_PRECONDITION.withDescription(msg).asRuntimeException();
    return v;
  }

  private static Timestamp ts(long ms) {
    return Timestamp.newBuilder().setSeconds(ms / 1000).setNanos((int) ((ms % 1000) * 1_000_000)).build();
  }

  private final class InsertContext implements AutoCloseable {

    protected Counts totals;

    final InsertOptions opts;

    final Database db;

    final List<String> keyCols;
    final List<String> updateCols;
    final Set<String> keyColsSet;

    // Mutable latch (single-stream, no concurrent access): set once after the "out/in in
    // update_columns ignored for edges" warning fires so it is logged at most once per stream.
    boolean warnedEdgeEndpointUpdateCols;

    long startedAt;

    final String sessionId = UUID.randomUUID().toString();
    long received = 0;

    // Issue #4644: ArcadeDB transactions are bound to the calling thread via the DatabaseContext
    // ThreadLocal. The gRPC serializing executor may run onNext (which calls db.begin()) and
    // onCompleted (which calls db.commit()) on different pool threads, so the transaction begun on
    // one callback thread must be explicitly re-bound onto whichever thread runs the next callback,
    // otherwise the deferred commit fails with "Transaction not begun". We capture the active
    // transaction and its security user here and re-apply them in bindToCurrentThread().
    private com.arcadedb.database.TransactionContext tx;
    private SecurityDatabaseUser                     txUser;

    InsertContext(InsertOptions opts) {

      this.opts = opts;

      this.db = getDatabase(opts.getDatabase(), opts.getCredentials());

      this.keyCols = opts.getKeyColumnsList();
      this.keyColsSet = Set.copyOf(this.keyCols);

      this.updateCols = opts.getUpdateColumnsOnConflictList();

      if (!opts.getValidateOnly()) {
        if (opts.getTransactionMode() == TransactionMode.PER_STREAM || opts.getTransactionMode() == TransactionMode.PER_BATCH
            || opts.getTransactionMode() == TransactionMode.PER_REQUEST || opts.getTransactionMode() == TransactionMode.PER_ROW) {
          db.begin();
        }
      }

      // Issue #4806: if anything after db.begin() throws, the caller never receives this context (it is
      // not yet stored in ctxRef), so its mid-stream failure path cannot roll the just-begun transaction
      // back - it would leak, bound to this pooled gRPC thread. Clean up our own transaction before
      // propagating the failure.
      try {
        captureTransaction();
      } catch (final RuntimeException e) {
        abortTransaction();
        throw e;
      }
    }

    /**
     * Issue #4644: re-bind the transaction begun on a previous callback thread onto the thread that
     * is about to run the current callback. gRPC serializes a call's StreamObserver callbacks but
     * does not pin them to a single thread, so begin/insert/commit can land on different pool
     * threads. This is a no-op when the transaction is already bound to the current thread (the
     * common no-hop case), so it never rolls back its own active transaction.
     */
    void bindToCurrentThread() {
      if (tx == null)
        return;

      final DatabaseContext.DatabaseContextTL tl = DatabaseContext.INSTANCE.getContextIfExists(db.getDatabasePath());
      if (tl != null && tl.getLastTransaction() == tx)
        return; // already bound to this thread

      final DatabaseContext.DatabaseContextTL rebound = DatabaseContext.INSTANCE.init((DatabaseInternal) db, tx);
      rebound.setCurrentUser(txUser);
    }

    /**
     * Issue #5041 (TX-4): detach the captured transaction from the current thread's
     * {@link DatabaseContext} WITHOUT rolling it back, so a still-open insert-stream transaction is
     * never left parked on a shared gRPC pool thread between callbacks. Left parked, an unrelated
     * request that reuses the same pool thread calls {@link DatabaseContext#init(DatabaseInternal)},
     * which rolls back whatever transaction it finds bound to the thread - silently discarding this
     * stream's rows. {@link #bindToCurrentThread()} re-attaches the transaction on the next callback.
     * The captured {@code tx} handle keeps the transaction (and its buffered rows) alive across the
     * detach. No-op when there is no captured transaction or it is not bound to this thread.
     */
    void unbindFromCurrentThread() {
      if (tx == null)
        return;

      final DatabaseContext.DatabaseContextTL tl = DatabaseContext.INSTANCE.getContextIfExists(db.getDatabasePath());
      if (tl != null && tl.getLastTransaction() == tx)
        DatabaseContext.INSTANCE.removeContext(db.getDatabasePath());
    }

    /**
     * Issue #4644: snapshot the transaction (and its security user) currently bound to this thread so
     * it can be re-bound on the next callback thread by {@link #bindToCurrentThread()}.
     */
    private void captureTransaction() {
      final DatabaseContext.DatabaseContextTL tl = DatabaseContext.INSTANCE.getContextIfExists(db.getDatabasePath());
      if (tl != null) {
        tx = tl.getLastTransaction();
        txUser = tl.getCurrentUser();
      } else {
        tx = null;
        txUser = null;
      }
    }

    /**
     * Issue #4806: abort the in-flight transaction after a transaction-level failure. Rolls back the
     * transaction if it is still active on the calling thread - which releases its locks and clears
     * this thread's binding - and drops the captured handle so the (now dead) transaction is never
     * re-bound onto a pooled gRPC thread. Idempotent and best-effort: a mid-stream commit failure has
     * usually already terminated the transaction (isTransactionActive() == false), in which case this
     * only clears the handle. Must be called on the thread whose context holds the transaction.
     *
     * @return {@code true} if an active transaction was actually rolled back (its uncommitted rows are
     *         no longer in the database); {@code false} if there was nothing to roll back (the
     *         transaction had already been terminated by a failed commit, so earlier batches persisted).
     */
    boolean abortTransaction() {
      try {
        if (db.isTransactionActive()) {
          db.rollback();
          return true;
        }
        return false;
      } catch (final Exception ignore) {
        // best-effort: the engine may have already rolled the transaction back on the failed commit
        return false;
      } finally {
        tx = null;
      }
    }

    void flushCommit(boolean end) {

      if (opts.getValidateOnly()) {
        if (end) {
          db.rollback();
          tx = null;
        }
        return;
      }
      switch (opts.getTransactionMode()) {
        case PER_ROW -> {
          db.commit();
          if (!end) {
            db.begin();
            captureTransaction();
          } else
            tx = null;
        }
        case PER_REQUEST -> {
          if (end) {
            db.commit();
            tx = null;
          }
        }
        case PER_BATCH -> {
          db.commit();
          if (!end) {
            db.begin();
            captureTransaction();
          } else
            tx = null;
        }
        case PER_STREAM -> {
          if (end) {
            db.commit();
            tx = null;
          }
        }
        default -> {
        }
      }
    }

    InsertSummary summary(Counts c, long startedAtMs) {

      long now = System.currentTimeMillis();

      return InsertSummary.newBuilder().setReceived(c.received).setInserted(c.inserted).setUpdated(c.updated).setIgnored(c.ignored)
          .setFailed(c.failed).addAllErrors(c.errors).setStartedAt(ts(startedAtMs)).setFinishedAt(ts(now)).build();
    }

    /**
     * Issue #5041 (TX-3): roll back any still-active captured transaction. Previously this was an
     * empty no-op, so {@link #closeQuietly()} on every error/cancel path (insertStream.onError, the
     * cancel handler, insertBidirectional cleanup, bulkInsert try-with-resources) left the
     * transaction begun in the ctor active and bound to a pooled gRPC thread. It is not tracked in
     * {@code activeTransactions}, so the reaper cannot reclaim it either; the next unrelated request
     * reusing that thread would silently join the orphaned transaction. Rebind the transaction onto
     * this thread first (it may have been detached by {@link #unbindFromCurrentThread()} or begun on
     * another callback thread) so the rollback releases its locks. Idempotent: after a successful
     * {@link #flushCommit(boolean)} the transaction handle is already {@code null} and this is a
     * no-op.
     * <p>
     * When a transaction WAS captured, {@link #bindToCurrentThread()} above binds it to the current
     * thread, so the current thread's {@link DatabaseContext} entry is provably ours: drop it to
     * release the stale per-thread state (currentUser, querySession, temporary buffers) left on the
     * pooled gRPC thread. We deliberately do NOT remove the context on the {@code tx == null} path:
     * {@code close()} can then be running on a different pooled callback thread (e.g. the
     * {@code streamFailed} branch of {@code insertStream.onCompleted}), where the entry may belong to
     * an unrelated request - the same "don't touch a foreign pooled thread's transaction" caution the
     * surrounding code observes.
     */
    @Override
    public void close() {
      final boolean hadTransaction = tx != null;
      try {
        if (hadTransaction) {
          bindToCurrentThread();
          abortTransaction();
        }
      } catch (final RuntimeException ignore) {
        // best-effort cleanup: the engine may already have terminated the transaction
      } finally {
        tx = null;
        if (hadTransaction)
          DatabaseContext.INSTANCE.removeContext(db.getDatabasePath());
      }
    }

    void closeQuietly() {
      try {
        close();
      } catch (Exception ignore) {
      }
    }
  }

  // Helper methods

  private Database getDatabase(String databaseName, DatabaseCredentials credentials) {

    // Reject path-traversal / path-bearing database names before any filesystem access, so a
    // request-supplied name can never escape the configured databases directory.
    validateDatabaseName(databaseName);

    // Authenticate the caller and authorize access to the requested database.
    validateCredentials(credentials, databaseName);

    // Use the same approach as Postgres/Redis plugins
    if (arcadeServer != null) {

      // This is how other plugins do it - get the already-open database
      Database db = arcadeServer.getDatabase(databaseName);

      LogManager.instance().log(this, Level.FINE, "getDatabase(): db = %s isOpen = %s", db, db.isOpen());

      if (db != null) {
        // Initialize the DatabaseContext thread-local for this thread (required by ArcadeDB's
        // transaction management). Other wire protocols (HTTP, Postgres) do this as well.
        // Always call init() to ensure the context references the correct database instance,
        // since gRPC reuses threads from its pool and a stale context may exist.
        final DatabaseContext.DatabaseContextTL ctx = DatabaseContext.INSTANCE.init((DatabaseInternal) db);

        // Propagate the authenticated user so HA-Raft can forward commands to the leader:
        // forwardCommandToLeaderViaRaft reads proxied.getCurrentUserName() to set the
        // X-ArcadeDB-Forwarded-User header. Without this, every command issued through a
        // follower throws "Cannot forward command to leader: no authenticated user in the
        // current security context". Mirrors PostgresNetworkExecutor.openDatabase().
        final String authenticatedUser = resolvedUsername(credentials);
        if (authenticatedUser != null && arcadeServer.getSecurity() != null) {
          final ServerSecurityUser secUser = arcadeServer.getSecurity().getUser(authenticatedUser);
          if (secUser != null)
            ctx.setCurrentUser(secUser.getDatabaseUser(db));
          else
            // A race between credential validation and concurrent user mutation can land here.
            // Leaving the user unset would silently break HA leader-forwarding, so surface it.
            LogManager.instance().log(this, Level.WARNING,
                "gRPC request authenticated as '%s' but server security has no matching user; HA leader-forwarding will fail for this request",
                authenticatedUser);
        }

        return db;
      }
    }

    // Check if database is already in the pool
    String poolKey = databaseName;
    Database database = databasePool.get(poolKey);

    if (database != null && database.isOpen()) {
      // Return existing open database
      return database;
    }

    // Create new database connection
    synchronized (databasePool) {
      // Double-check after acquiring lock
      database = databasePool.get(poolKey);
      if (database != null && database.isOpen()) {
        return database;
      }

      // Create database factory for the specific database
      DatabaseFactory dbFactory = new DatabaseFactory(databasePath + "/" + databaseName);

      try {
        // Open database - ArcadeDB requires MODE parameter
        if (dbFactory.exists()) {
          // Try READ_ONLY first to avoid conflicts
          try {
            database = dbFactory.open(ComponentFile.MODE.READ_ONLY);
          } catch (Exception e) {
            // If READ_ONLY fails, try READ_WRITE
            LogManager.instance().log(this, Level.FINE, "Opening database in READ_WRITE mode: %s", databaseName);
            database = dbFactory.open(ComponentFile.MODE.READ_WRITE);
          }
        } else {
          // Create if it doesn't exist
          database = dbFactory.create();
        }

        // Add to pool
        databasePool.put(poolKey, database);
        return database;

      } catch (Exception e) {
        LogManager.instance().log(this, Level.SEVERE, "Failed to open database: %s", e, databaseName);
        throw new RuntimeException("Cannot open database: " + databaseName + " - " + e.getMessage(), e);
      }
    }
  }

  /**
   * Resolves the authenticated username for the current request. Prefers the interceptor-set
   * gRPC context (Bearer or basic auth flows) and falls back to the credentials carried by the
   * request payload. Single source of truth for username precedence; both
   * {@link #validateCredentials(DatabaseCredentials, String)} and {@link #getDatabase(String, DatabaseCredentials)}
   * delegate here so the rule cannot drift between callers.
   */
  private String resolvedUsername(final DatabaseCredentials credentials) {
    final String contextUser = GrpcAuthInterceptor.USER_CONTEXT_KEY.get();
    if (contextUser != null && !contextUser.isEmpty())
      return contextUser;
    if (credentials != null) {
      final String credUser = credentials.getUsername();
      if (credUser != null && !credUser.isEmpty())
        return credUser;
    }
    return null;
  }

  /**
   * Rejects database names that could escape the configured databases directory. A request-supplied
   * name containing a path separator ({@code /} or {@code \}) or a parent reference ({@code ..})
   * would otherwise be concatenated straight onto the databases path and let a caller open or create
   * databases anywhere on the filesystem (path traversal). Mirrors the validation already used by the
   * HTTP server handlers (e.g. {@code PostServerCommandHandler}).
   */
  private static void validateDatabaseName(final String databaseName) {
    if (databaseName == null || databaseName.isBlank())
      throw Status.INVALID_ARGUMENT.withDescription("Invalid database name: name is required").asRuntimeException();
    // A bare "." would resolve to the databases directory itself, so it is rejected alongside the
    // separator and parent-reference checks.
    if (".".equals(databaseName) || databaseName.contains("/") || databaseName.contains("\\") || databaseName.contains(".."))
      throw Status.INVALID_ARGUMENT.withDescription("Invalid database name: " + databaseName).asRuntimeException();
  }

  /**
   * Authenticates the caller and authorizes access to the named database.
   * <p>
   * A resolvable username alone is NOT proof of identity. When server security is active (at least
   * one user is configured) this enforces real authentication and per-database authorization:
   * <ul>
   *   <li>if the auth interceptor already verified this connection's credentials (context user set),
   *       the resolved user is authorized for the requested database;</li>
   *   <li>otherwise the request-payload username/password are authenticated and the requested
   *       database authorized in a single {@link ServerSecurity#authenticate} call.</li>
   * </ul>
   * When no users are configured the server is intentionally open (security disabled) and only the
   * presence of a username is required, matching the auth interceptor's {@code securityEnabled} gate.
   */
  private void validateCredentials(final DatabaseCredentials credentials, final String databaseName) {
    final String authenticatedUser = resolvedUsername(credentials);
    if (authenticatedUser == null)
      throw Status.UNAUTHENTICATED.withDescription("Invalid credentials").asRuntimeException();

    final ServerSecurity security = arcadeServer != null ? arcadeServer.getSecurity() : null;
    if (security != null && security.getUsers() != null && !security.getUsers().isEmpty()) {
      final String contextUser = GrpcAuthInterceptor.USER_CONTEXT_KEY.get();
      if (contextUser != null && !contextUser.isEmpty()) {
        // The auth interceptor already verified the password for this connection; only authorize the
        // already-authenticated user for the specific database named in the request.
        final ServerSecurityUser user = security.getUser(contextUser);
        if (user == null)
          throw Status.UNAUTHENTICATED.withDescription("User/Password not valid").asRuntimeException();
        final Set<String> allowedDatabases = user.getAuthorizedDatabases();
        if (!allowedDatabases.contains(SecurityManager.ANY) && !allowedDatabases.contains(databaseName))
          throw Status.PERMISSION_DENIED.withDescription("User has not access to database '" + databaseName + "'")
              .asRuntimeException();
      } else {
        // No interceptor context (request-payload credentials only): authenticate the
        // username/password pair and authorize the requested database in one step.
        final String password = credentials != null ? credentials.getPassword() : null;
        if (password == null || password.isEmpty())
          throw Status.UNAUTHENTICATED.withDescription("Authentication required").asRuntimeException();
        try {
          security.authenticate(authenticatedUser, password, databaseName);
        } catch (final ServerSecurityException e) {
          throw Status.PERMISSION_DENIED.withDescription(e.getMessage()).asRuntimeException();
        }
      }
    }

    LogManager.instance().log(this, Level.FINE, "validateCredentials(): resolved user '%s'", authenticatedUser);
  }

  /**
   * Converts a Result to GrpcRecord, preserving all properties including aliases.
   * This method works at the Result level (not Record level) to maintain alias information.
   *
   * @param result           the Result object from a query execution
   * @param db               the database instance
   * @param projectionConfig optional projection configuration
   * @return GrpcRecord with all properties and aliases preserved
   */
  private GrpcRecord convertResultToGrpcRecord(Result result, Database db, ProjectionConfig projectionConfig) {
    GrpcRecord.Builder builder = GrpcRecord.newBuilder();

    // If this result wraps an element (Document/Vertex/Edge), get its metadata
    if (result.isElement()) {
      Document dbRecord = result.toElement();

      if (dbRecord.getIdentity() != null) {
        builder.setRid(dbRecord.getIdentity().toString());
      }

      if (dbRecord.getType() != null) {
        builder.setType(dbRecord.getTypeName());
      }
    }

    // HA-forwarded rows are non-element projections; recover the type from the @type property.
    if (builder.getType().isEmpty()) {
      final Object typeProperty = result.getProperty(Property.TYPE_PROPERTY);
      if (typeProperty instanceof String typeName && !typeName.isEmpty())
        builder.setType(typeName);
    }

    // Iterate over ALL properties from the Result, including aliases.
    // Null-valued projections must be included (unset GrpcValue) so clients can always
    // address every projected alias by key, matching the HTTP serializer's {"key": null} behavior.
    for (String propertyName : result.getPropertyNames()) {
      final Object value = result.getProperty(propertyName);

      final boolean debug = LogManager.instance().isDebugEnabled();

      if (debug)
        LogManager.instance()
            .log(this, Level.FINE, "convertResultToGrpcRecord(): Converting %s\n  value = %s\n  class = %s",
                propertyName, value, value == null ? "null" : value.getClass().getName());

      final GrpcValue gv = projectionConfig != null ?
          toGrpcValue(value, projectionConfig) :
          toGrpcValue(value);

      if (debug)
        LogManager.instance()
            .log(this, Level.FINE, "ENC-RES %s: %s -> %s", propertyName, summarizeJava(value), summarizeGrpc(gv));

      builder.putProperties(propertyName, gv);
    }

    // Ensure @rid and @type are always in the properties map when there's an element
    // This matches JsonSerializer behavior and works around client-side limitations
    if (result.isElement()) {
      final Document document = result.toElement();

      if (!builder.getPropertiesMap().containsKey(Property.RID_PROPERTY) && document.getIdentity() != null) {
        builder.putProperties(Property.RID_PROPERTY, toGrpcValue(document.getIdentity()));
      }

      if (!builder.getPropertiesMap().containsKey(Property.TYPE_PROPERTY) && document instanceof Document doc
          && doc.getType() != null) {
        builder.putProperties(Property.TYPE_PROPERTY, toGrpcValue(doc.getTypeName()));
      }
    }

    // If this is an Edge and @out/@in are not already in properties, add them
    if (result.isElement() && result.getElement().get() instanceof Edge edge) {
      if (!builder.getPropertiesMap().containsKey("@out")) {
        builder.putProperties("@out", toGrpcValue(edge.getOut().getIdentity()));
      }
      if (!builder.getPropertiesMap().containsKey("@in")) {
        builder.putProperties("@in", toGrpcValue(edge.getIn().getIdentity()));
      }
    }

    LogManager.instance().log(this, Level.FINE, "ENC-RES DONE rid=%s type=%s props=%s",
        builder.getRid(), builder.getType(), builder.getPropertiesCount());

    return builder.build();
  }

  private GrpcRecord convertToGrpcRecord(Record dbRecord, Database db) {

    GrpcRecord.Builder builder = GrpcRecord.newBuilder().setRid(dbRecord.getIdentity().toString());

    if (dbRecord instanceof Document doc) {

      if (doc.getType() != null)
        builder.setType(doc.getTypeName());

      if (doc.getIdentity() != null)
        builder.setRid(doc.getIdentity().toString());

      // set all properties
      for (String propertyName : doc.getPropertyNames()) {

        Object value = doc.get(propertyName);

        if (value != null) {

          final boolean debug = LogManager.instance().isDebugEnabled();

          if (debug)
            LogManager.instance()
                .log(this, Level.FINE, "convertToGrpcRecord(): Converting %s\n  value = %s\n  class = %s", propertyName
                    , value,
                    value.getClass());

          GrpcValue gv = toGrpcValue(value);

          if (debug)
            LogManager.instance()
                .log(this, Level.FINE, "ENC-REC %s.%s: %s -> %s", builder.getRid(), propertyName, summarizeJava(value),
                    summarizeGrpc(gv));

          builder.putProperties(propertyName, gv);
        }
      }

      // If this is an Edge, include @out / @in for convenience (string rid or link)
      if (dbRecord instanceof Edge edge) {

        if (!builder.getPropertiesMap().containsKey("@out")) {
          builder.putProperties("@out", toGrpcValue(edge.getOut().getIdentity()));
        }

        if (!builder.getPropertiesMap().containsKey("@in")) {
          builder.putProperties("@in", toGrpcValue(edge.getIn().getIdentity()));
        }
      }
    }

    LogManager.instance().log(this, Level.FINE, "ENC-REC DONE rid=%s type=%s props=%s", builder.getRid(),
        builder.getType(),
        builder.getPropertiesCount());

    return builder.build();
  }

  private GrpcValue convertPropToGrpcValue(String propName, Result result, ProjectionConfig pc) {

    final Object propValue = result.getProperty(propName);

    if (LogManager.instance().isDebugEnabled())
      LogManager.instance()
          .log(this, Level.FINE, "convertPropToGrpcValue(): Converting %s\n  value = %s\n  class = %s", propName,
              propValue,
              propValue == null ? "null" : propValue.getClass());

    return toGrpcValue(propValue, pc);
  }

  private GrpcValue convertPropToGrpcValue(String propName, Result result) {

    final Object propValue = result.getProperty(propName);

    if (LogManager.instance().isDebugEnabled())
      LogManager.instance()
          .log(this, Level.FINE, "convertPropToGrpcValue(): Converting %s\n  value = %s\n  class = %s", propName,
              propValue,
              propValue == null ? "null" : propValue.getClass());

    return toGrpcValue(propValue);
  }

  private Object toJavaForProperty(final Database db, final MutableDocument parent, final DocumentType dtype,
                                   final String propName,
                                   final GrpcValue grpcValue) {

    if (grpcValue == null)
      return null;

    // Issue #4263: a property explicitly set to null travels over the wire as a GrpcValue with no
    // kind set. Treat it as a real null regardless of the declared schema type, otherwise the STRING
    // branch below would store the literal string "null" (String.valueOf(null)).
    if (grpcValue.getKindCase() == GrpcValue.KindCase.KIND_NOT_SET)
      return null;

    // Try schema
    Property prop = null;
    try {
      prop = dtype != null ? dtype.getProperty(propName) : null;
    } catch (SchemaException ignore) {
    }

    if (prop != null)
      return convertWithSchemaType(db, parent, prop, propName, grpcValue);

    // No schema → generic
    return fromGrpcValue(grpcValue);
  }

  private Object convertWithSchemaType(Database db, MutableDocument parent, Property prop, String propName,
                                       GrpcValue v) {

    var t = prop.getType();

    switch (t) {

      case BOOLEAN:
        return switch (v.getKindCase()) {
          case BOOL_VALUE -> v.getBoolValue();
          case STRING_VALUE -> Boolean.parseBoolean(v.getStringValue());
          default -> null;
        };

      case BYTE:
        return switch (v.getKindCase()) {
          case INT32_VALUE, INT64_VALUE, DOUBLE_VALUE, FLOAT_VALUE -> (byte) (long) fromGrpcValue(v);
          case STRING_VALUE -> Byte.parseByte(v.getStringValue());
          default -> null;
        };

      case SHORT:
        return switch (v.getKindCase()) {
          case INT32_VALUE, INT64_VALUE, DOUBLE_VALUE, FLOAT_VALUE -> (short) (long) fromGrpcValue(v);
          case STRING_VALUE -> Short.parseShort(v.getStringValue());
          default -> null;
        };

      case INTEGER:
        return switch (v.getKindCase()) {
          case INT32_VALUE -> v.getInt32Value();
          case INT64_VALUE -> (int) v.getInt64Value();
          case DOUBLE_VALUE -> (int) v.getDoubleValue();
          case FLOAT_VALUE -> (int) v.getFloatValue();
          case STRING_VALUE -> Integer.parseInt(v.getStringValue());
          default -> null;
        };

      case LONG:
        return switch (v.getKindCase()) {
          case INT64_VALUE -> v.getInt64Value();
          case INT32_VALUE -> (long) v.getInt32Value();
          case DOUBLE_VALUE -> (long) v.getDoubleValue();
          case FLOAT_VALUE -> (long) v.getFloatValue();
          case STRING_VALUE -> Long.parseLong(v.getStringValue());
          default -> null;
        };

      case FLOAT:
        return switch (v.getKindCase()) {
          case FLOAT_VALUE -> v.getFloatValue();
          case DOUBLE_VALUE -> (float) v.getDoubleValue();
          case INT32_VALUE -> (float) v.getInt32Value();
          case INT64_VALUE -> (float) v.getInt64Value();
          case STRING_VALUE -> Float.parseFloat(v.getStringValue());
          default -> null;
        };

      case DOUBLE:
        return switch (v.getKindCase()) {
          case DOUBLE_VALUE -> v.getDoubleValue();
          case FLOAT_VALUE -> (double) v.getFloatValue();
          case INT32_VALUE -> (double) v.getInt32Value();
          case INT64_VALUE -> (double) v.getInt64Value();
          case STRING_VALUE -> Double.parseDouble(v.getStringValue());
          default -> null;
        };

      case STRING:
        return switch (v.getKindCase()) {
          case STRING_VALUE -> v.getStringValue();
          default -> String.valueOf(fromGrpcValue(v));
        };

      case DECIMAL:
        return switch (v.getKindCase()) {
          case DECIMAL_VALUE -> {
            var d = v.getDecimalValue();
            yield new BigDecimal(BigInteger.valueOf(d.getUnscaled()), d.getScale());
          }
          case STRING_VALUE -> new BigDecimal(v.getStringValue());
          case DOUBLE_VALUE -> BigDecimal.valueOf(v.getDoubleValue());
          case INT32_VALUE -> BigDecimal.valueOf(v.getInt32Value());
          case INT64_VALUE -> BigDecimal.valueOf(v.getInt64Value());
          default -> null;
        };

      case DATE:
      case DATETIME:
        // Prefer timestamp_value; else accept epoch ms in int64; else parse string (epoch ms or ISO 8601)
        return switch (v.getKindCase()) {
          case TIMESTAMP_VALUE -> new Date(GrpcTypeConverter.tsToMillis(v.getTimestampValue()));
          case INT64_VALUE -> new Date(v.getInt64Value());
          case STRING_VALUE -> {
            final String s = v.getStringValue();
            try {
              yield new Date(Long.parseLong(s));
            } catch (NumberFormatException ignored) {
              final Long millis = DateUtils.dateTimeToTimestamp(db, s, ChronoUnit.MILLIS);
              yield millis != null ? new Date(millis) : null;
            }
          }
          default -> null;
        };

      case BINARY:
        return switch (v.getKindCase()) {
          case BYTES_VALUE -> v.getBytesValue().toByteArray();
          case STRING_VALUE -> v.getStringValue().getBytes(StandardCharsets.UTF_8);
          default -> null;
        };

      case LINK:
        return switch (v.getKindCase()) {
          case LINK_VALUE -> new RID(v.getLinkValue().getRid());
          case STRING_VALUE -> new RID(v.getStringValue());
          default -> null;
        };

      case EMBEDDED: {

        // Use schema ofType if present; otherwise use embedded.type from the payload
        LogManager.instance()
            .log(this, Level.FINE, "EMBEDDED: prop='%s', incoming kind=%s, schema ofType='%s'", propName,
                v.getKindCase(),
                prop.getOfType());

        String embeddedTypeName =
            v.getKindCase() == GrpcValue.KindCase.EMBEDDED_VALUE && !v.getEmbeddedValue().getType().isEmpty()
                ? v.getEmbeddedValue().getType()
                : prop.getOfType();

        // If still unknown, try to discover from MAP payload keys
        // (type/@type/empowerType)
        if ((embeddedTypeName == null || embeddedTypeName.isEmpty()) && v.getKindCase() == GrpcValue.KindCase.MAP_VALUE) {
          var entries = v.getMapValue().getEntriesMap();
          GrpcValue tv = entries.getOrDefault("type", entries.getOrDefault("@type", entries.get("empowerType")));
          if (tv != null && tv.getKindCase() == GrpcValue.KindCase.STRING_VALUE) {
            embeddedTypeName = tv.getStringValue();
            LogManager.instance()
                .log(this, Level.FINE, "EMBEDDED: discovered embeddedTypeName='%s' from MAP payload", embeddedTypeName);
          }
        }

        // If we still don't know the type and payload isn't EMBEDDED, fall back to Map
        if ((embeddedTypeName == null || embeddedTypeName.isEmpty()) && v.getKindCase() != GrpcValue.KindCase.EMBEDDED_VALUE) {

          LogManager.instance()
              .log(this, Level.FINE, "EMBEDDED: fallback to Map for prop='%s' (kind=%s, embeddedTypeName='%s')",
                  propName,
                  v.getKindCase(),
                  embeddedTypeName);
          return fromGrpcValue(v);
        }

        final String source =
            v.getKindCase() == GrpcValue.KindCase.EMBEDDED_VALUE && !v.getEmbeddedValue().getType().isEmpty()
                ? "payload"
                : "schema/discovered";
        int fields = v.getKindCase() == GrpcValue.KindCase.EMBEDDED_VALUE ? v.getEmbeddedValue().getFieldsCount()
            : v.getKindCase() == GrpcValue.KindCase.MAP_VALUE ? v.getMapValue().getEntriesCount() : 0;
        LogManager.instance()
            .log(this, Level.FINE, "EMBEDDED: resolved embeddedTypeName='%s' (source=%s), fields=%s",
                embeddedTypeName, source,
                fields);

        // Build typed embedded document
        MutableEmbeddedDocument ed = parent.newEmbeddedDocument(embeddedTypeName, propName);
        DocumentType embeddedType = null;
        try {
          embeddedType = db.getSchema().getType(embeddedTypeName);
          LogManager.instance().log(this, Level.FINE, "EMBEDDED: schema type lookup '%s' -> %s", embeddedTypeName,
              embeddedType != null ? "FOUND" : "NOT FOUND");
        } catch (Exception e) {
          LogManager.instance()
              .log(this, Level.FINE, "EMBEDDED: schema type lookup failed for '%s': %s", embeddedTypeName,
                  e.toString());
        }
        final DocumentType embeddedTypeFinal = embeddedType;

        // Populate from either EMBEDDED_VALUE.fields or MAP_VALUE.entries
        switch (v.getKindCase()) {
          case EMBEDDED_VALUE:
            v.getEmbeddedValue().getFieldsMap().forEach((k, vv) -> {
              LogManager.instance().log(this, Level.FINE, "EMBEDDED: field '%s' raw kind=%s", k, vv.getKindCase());
              Object j = embeddedTypeFinal != null ? toJavaForProperty(db, ed, embeddedTypeFinal, k, vv) :
                  fromGrpcValue(vv);
              LogManager.instance().log(this, Level.FINE, "EMBEDDED: field '%s' converted -> %s", k,
                  j == null ? "null" : j.getClass().getSimpleName());
              ed.set(k, j);
            });
            break;

          case MAP_VALUE:
            v.getMapValue().getEntriesMap().forEach((k, vv) -> {
              LogManager.instance().log(this, Level.FINE, "EMBEDDED: field '%s' raw kind=%s", k, vv.getKindCase());
              Object j = embeddedTypeFinal != null ? toJavaForProperty(db, ed, embeddedTypeFinal, k, vv) :
                  fromGrpcValue(vv);
              LogManager.instance().log(this, Level.FINE, "EMBEDDED: field '%s' converted -> %s", k,
                  j == null ? "null" : j.getClass().getSimpleName());
              ed.set(k, j);
            });
            break;

          default:
            // Shouldn't happen; be safe
            LogManager.instance()
                .log(this, Level.FINE, "EMBEDDED: unexpected kind=%s, falling back to Map for prop='%s'",
                    v.getKindCase(), propName);
            return fromGrpcValue(v);
        }

        LogManager.instance()
            .log(this, Level.FINE, "EMBEDDED: built embedded doc type='%s' for prop='%s'", embeddedTypeName, propName);
        return ed;
      }

      case MAP:
        if (v.getKindCase() == GrpcValue.KindCase.MAP_VALUE) {
          var m = new LinkedHashMap<String, Object>();
          v.getMapValue().getEntriesMap().forEach((k, vv) -> m.put(k, fromGrpcValue(vv)));
          return m;
        }
        return null;

      case LIST:
        if (v.getKindCase() == GrpcValue.KindCase.LIST_VALUE) {
          var list = new ArrayList<>();
          for (GrpcValue item : v.getListValue().getValuesList()) {
            list.add(fromGrpcValue(item));
          }
          return list;
        }
        return null;
      case ARRAY_OF_SHORTS: {
        if (v.getKindCase() == GrpcValue.KindCase.LIST_VALUE) {
          var out = new ArrayList<Short>();
          for (GrpcValue item : v.getListValue().getValuesList()) {
            Object o = fromGrpcValue(item);
            if (o instanceof Number n)
              out.add(n.shortValue());
            else if (o instanceof String s)
              out.add(Short.parseShort(s));
          }
          return out;
        }
        return null;
      }

      case ARRAY_OF_INTEGERS: {
        if (v.getKindCase() == GrpcValue.KindCase.LIST_VALUE) {
          var out = new ArrayList<Integer>();
          for (GrpcValue item : v.getListValue().getValuesList()) {
            Object o = fromGrpcValue(item);
            if (o instanceof Number n)
              out.add(n.intValue());
            else if (o instanceof String s)
              out.add(Integer.parseInt(s));
          }
          return out;
        }
        return null;
      }

      case ARRAY_OF_LONGS: {
        if (v.getKindCase() == GrpcValue.KindCase.LIST_VALUE) {
          var out = new ArrayList<Long>();
          for (GrpcValue item : v.getListValue().getValuesList()) {
            Object o = fromGrpcValue(item);
            if (o instanceof Number n)
              out.add(n.longValue());
            else if (o instanceof String s)
              out.add(Long.parseLong(s));
          }
          return out;
        }
        return null;
      }

      case ARRAY_OF_FLOATS: {
        if (v.getKindCase() == GrpcValue.KindCase.LIST_VALUE) {
          var out = new ArrayList<Float>();
          for (GrpcValue item : v.getListValue().getValuesList()) {
            Object o = fromGrpcValue(item);
            if (o instanceof Number n)
              out.add(n.floatValue());
            else if (o instanceof String s)
              out.add(Float.parseFloat(s));
          }
          return out;
        }
        return null;
      }

      case ARRAY_OF_DOUBLES: {
        if (v.getKindCase() == GrpcValue.KindCase.LIST_VALUE) {
          var out = new ArrayList<Double>();
          for (GrpcValue item : v.getListValue().getValuesList()) {
            Object o = fromGrpcValue(item);
            if (o instanceof Number n)
              out.add(n.doubleValue());
            else if (o instanceof String s)
              out.add(Double.parseDouble(s));
          }
          return out;
        }
        return null;
      }

      case DATETIME_SECOND:
      case DATETIME_MICROS:
      case DATETIME_NANOS: {
        // Keep full nanosecond precision from the proto Timestamp (matching the parameter-binding
        // path fromGrpcValue). Type.convert() truncates the Instant to the column's declared
        // precision via DateUtils.getPrecisionFromType(...); a java.util.Date would collapse the
        // value to milliseconds up front, discarding the sub-millisecond digits these types keep.
        return switch (v.getKindCase()) {
          case TIMESTAMP_VALUE -> GrpcTypeConverter.tsToInstant(v.getTimestampValue());
          case INT64_VALUE -> new Date(v.getInt64Value()); // epoch ms expected
          case STRING_VALUE -> new Date(Long.parseLong(v.getStringValue()));
          default -> null;
        };
      }
    }

    // default fallback
    return fromGrpcValue(v);
  }

  private String generateTransactionId() {
    // UUID (CSPRNG-backed) instead of System.nanoTime(): high-entropy so it cannot be enumerated by an
    // attacker guessing values around a known point in time, and collision-free so two concurrent begins
    // can never map to the same id and drive one another's server-side transaction.
    return "tx_" + UUID.randomUUID();
  }

  private static String langOrDefault(String language) {
    return language == null || language.isEmpty() ? "sql" : language;
  }

  // ---- Debug helpers ----
  private static String summarizeJava(Object o) {
    if (o == null)
      return "null";
    try {
      if (o instanceof CharSequence s)
        return "String(" + s.length() + ")=\"" + (s.length() > 120 ? s.subSequence(0, 120) + "…" : s) + "\"";
      if (o instanceof byte[] b)
        return "bytes[" + b.length + "]";
      if (o instanceof Collection<?> c)
        return o.getClass().getSimpleName() + "[size=" + c.size() + "]";
      if (o instanceof Map<?, ?> m)
        return o.getClass().getSimpleName() + "[size=" + m.size() + "]";
      return o.getClass().getSimpleName() + "(" + String.valueOf(o) + ")";
    } catch (Exception e) {
      return o.getClass().getSimpleName();
    }
  }

  private static String summarizeGrpc(GrpcValue v) {
    if (v == null)
      return "GrpcValue(null)";
    switch (v.getKindCase()) {
      case BOOL_VALUE:
        return "BOOL(" + v.getBoolValue() + ")";
      case INT32_VALUE:
        return "INT32(" + v.getInt32Value() + ")";
      case INT64_VALUE:
        return "INT64(" + v.getInt64Value() + ")";
      case FLOAT_VALUE:
        return "FLOAT(" + v.getFloatValue() + ")";
      case DOUBLE_VALUE:
        return "DOUBLE(" + v.getDoubleValue() + ")";
      case STRING_VALUE: {
        String s = v.getStringValue();
        return "STRING(" + s.length() + ")=\"" + (s.length() > 120 ? s.substring(0, 120) + "…" : s) + "\"";
      }
      case BYTES_VALUE:
        return "BYTES[" + v.getBytesValue().size() + "]";
      case TIMESTAMP_VALUE:
        return "TIMESTAMP(" + v.getTimestampValue().getSeconds() + "." + v.getTimestampValue().getNanos() + ")";
      case LIST_VALUE:
        return "LIST[size=" + v.getListValue().getValuesCount() + "]";
      case MAP_VALUE:
        return "MAP[size=" + v.getMapValue().getEntriesCount() + "]";
      case EMBEDDED_VALUE:
        return "EMBEDDED[type=" + v.getEmbeddedValue().getType() + ", size=" + v.getEmbeddedValue().getFieldsCount() + "]";
      case LINK_VALUE:
        return "LINK(" + v.getLinkValue().getRid() + ")";
      case DECIMAL_VALUE:
        return "DECIMAL(unscaled=" + v.getDecimalValue().getUnscaled() + ", scale=" + v.getDecimalValue().getScale() + ")";
      case KIND_NOT_SET:
      default:
        return "GrpcValue(KIND_NOT_SET)";
    }
  }

  private GrpcValue dbgEnc(String where, Object in, GrpcValue out, String ctx) {
    // Guard the eager summary building so nothing is allocated at the default (non-debug) log level.
    if (LogManager.instance().isDebugEnabled())
      LogManager.instance()
          .log(this, Level.FINE, "GRPC-ENC [%s]%s in=%s -> out=%s", where, ctx == null ? "" : " " + ctx,
              summarizeJava(in),
              summarizeGrpc(out));
    return out;
  }

  private Object dbgDec(String where, GrpcValue in, Object out, String ctx) {
    // Guard the eager summary building so nothing is allocated at the default (non-debug) log level.
    if (LogManager.instance().isDebugEnabled())
      LogManager.instance()
          .log(this, Level.FINE, "GRPC-DEC [%s]%s in=%s -> out=%s", where, ctx == null ? "" : " " + ctx,
              summarizeGrpc(in),
              summarizeJava(out));
    return out;
  }

  // --- Gson structural encoder: JsonElement -> GrpcValue ---
  private GrpcValue gsonToGrpc(JsonElement n) {

    GrpcValue.Builder b = GrpcValue.newBuilder();

    if (n == null || n.isJsonNull()) {
      return b.build(); // KIND_NOT_SET => null on the client
    }

    if (n.isJsonPrimitive()) {
      JsonPrimitive p = n.getAsJsonPrimitive();
      if (p.isBoolean())
        return b.setBoolValue(p.getAsBoolean()).build();
      if (p.isNumber()) {
        BigDecimal bd;
        try {
          bd = p.getAsBigDecimal();
        } catch (Exception e) {
          return b.setDoubleValue(p.getAsDouble()).build();
        }
        if (bd.scale() == 0) {
          try {
            int i = bd.intValueExact();
            return b.setInt32Value(i).build();
          } catch (ArithmeticException ignore) {
          }
          try {
            long l = bd.longValueExact();
            return b.setInt64Value(l).build();
          } catch (ArithmeticException ignore) {
          }
          BigInteger unscaled = bd.unscaledValue();
          if (unscaled.bitLength() <= 63) {
            return b.setDecimalValue(GrpcDecimal.newBuilder().setUnscaled(unscaled.longValue()).setScale(bd.scale()).build())
                .build();
          }
          return b.setStringValue(bd.toPlainString()).build();
        } else {
          BigInteger unscaled = bd.unscaledValue();
          if (unscaled.bitLength() <= 63) {
            return b.setDecimalValue(GrpcDecimal.newBuilder().setUnscaled(unscaled.longValue()).setScale(bd.scale()).build())
                .build();
          }
          return b.setStringValue(bd.toPlainString()).build();
        }
      }
      if (p.isString())
        return b.setStringValue(p.getAsString()).build();
      return b.setStringValue(p.getAsString()).build();
    }

    if (n.isJsonArray()) {
      GrpcList.Builder lb = GrpcList.newBuilder();
      for (JsonElement e : n.getAsJsonArray()) {
        lb.addValues(gsonToGrpc(e));
      }
      return b.setListValue(lb.build()).build();
    }

    if (n.isJsonObject()) {
      GrpcMap.Builder mb = GrpcMap.newBuilder();
      JsonObject obj = n.getAsJsonObject();
      for (var entry : obj.entrySet()) {
        mb.putEntries(entry.getKey(), gsonToGrpc(entry.getValue()));
      }
      return b.setMapValue(mb.build()).build();
    }

    return b.setStringValue(n.toString()).build();
  }

  static final class ProjectionConfig {
    final boolean            include;
    final ProjectionEncoding enc;
    final int                softLimitBytes; // 0 = no soft limit

    // Running counter for MAP/JSON builds:
    final    AtomicInteger used      = new AtomicInteger(0);
    volatile boolean       truncated = false;

    ProjectionConfig(boolean include, ProjectionEncoding enc, int softLimitBytes) {
      this.include = include;
      this.enc = enc == null ? ProjectionEncoding.PROJECTION_AS_LINK : enc;
      this.softLimitBytes = Math.max(softLimitBytes, 0);
    }

    boolean wouldExceed(int add) {
      if (softLimitBytes <= 0)
        return false;
      int after = used.addAndGet(add);
      if (after > softLimitBytes) {
        truncated = true;
      }
      return after > softLimitBytes;
    }

    public boolean isInclude() {
      return include;
    }

    public ProjectionEncoding getEnc() {
      return enc;
    }
  }
}
