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

import com.arcadedb.database.Database;
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
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.VertexType;
import com.arcadedb.serializer.JsonSerializer;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.grpc.InsertOptions.ConflictMode;
import com.arcadedb.server.grpc.InsertOptions.TransactionMode;
import com.arcadedb.server.grpc.ProjectionSettings.ProjectionEncoding;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.jspecify.annotations.NonNull;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
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

    TransactionContext(Database db, String txId) {
      this.db = db;
      this.txId = txId;
      // Single-thread executor ensures all tx operations happen on the same thread
      this.executor = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "arcadedb-tx-" + txId);
        t.setDaemon(true);
        return t;
      });
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

  public ArcadeDbGrpcService(String databasePath, ArcadeDBServer server) {
    this.databasePath = databasePath;
    this.arcadeServer = server;
  }

  public void close() {
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
      final String incomingTxId = (hasTx && tx != null) ? tx.getTransactionId() : null;
      final TransactionContext txCtx = (incomingTxId != null && !incomingTxId.isBlank())
          ? activeTransactions.get(incomingTxId) : null;

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

    try {
      final Map<String, Object> params = GrpcTypeConverter.convertParameters(req.getParametersMap());

      // Language defaults to "sql" when empty
      final String language = (req.getLanguage() == null || req.getLanguage().isEmpty()) ? "sql" : req.getLanguage();

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

                // Convert Result to GrpcRecord, preserving aliases and all properties
                GrpcRecord grpcRecord = convertResultToGrpcRecord(result, db,
                    new ProjectionConfig(true, ProjectionEncoding.PROJECTION_AS_JSON, 0));
                out.addRecords(grpcRecord);

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
        }
      }

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
      out.setAffectedRecords(affected).setExecutionTimeMs(ms);
      return out.build();

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
    }
  }

  @Override
  public void createRecord(CreateRecordRequest req, StreamObserver<CreateRecordResponse> resp) {
    // Check for external transaction
    final String incomingTxId = req.hasTransaction() ? req.getTransaction().getTransactionId() : null;
    final TransactionContext txCtx = (incomingTxId != null && !incomingTxId.isBlank())
        ? activeTransactions.get(incomingTxId) : null;

    if (txCtx != null) {
      // External transaction — execute on its dedicated thread to maintain thread-local state
      try {
        final Future<CreateRecordResponse> future = txCtx.executor.submit(() -> createRecordInternal(req, txCtx.db));
        resp.onNext(future.get());
        resp.onCompleted();
      } catch (Exception e) {
        final Throwable cause = (e instanceof ExecutionException && e.getCause() != null) ? e.getCause() : e;
        LogManager.instance().log(this, Level.SEVERE, "ERROR in createRecord (external tx)", cause);
        resp.onError(Status.INTERNAL.withDescription("CreateRecord: " + cause.getMessage()).asException());
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
      resp.onError(Status.INVALID_ARGUMENT.withDescription("CreateRecord: " + e.getMessage()).asException());
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

    // --- Vertex ---
    if (dt instanceof VertexType) {
      final MutableVertex v = db.newVertex(cls);
      props.forEach((k, val) -> v.set(k, toJavaForProperty(db, v, dt, k, val)));
      v.save();
      return CreateRecordResponse.newBuilder().setRid(v.getIdentity().toString()).build();
    }

    // --- Edge ---
    if (dt instanceof EdgeType) {
      String outStr = null, inStr = null;

      if (props.containsKey("out")) {
        GrpcValue pv = props.get("out");
        outStr = (pv.getKindCase() == GrpcValue.KindCase.STRING_VALUE) ? pv.getStringValue() :
            String.valueOf(fromGrpcValue(pv));
      }

      if (props.containsKey("in")) {
        GrpcValue pv = props.get("in");
        inStr = (pv.getKindCase() == GrpcValue.KindCase.STRING_VALUE) ? pv.getStringValue() :
            String.valueOf(fromGrpcValue(pv));
      }

      if (outStr == null || inStr == null)
        throw new IllegalArgumentException("Edge requires 'out' and 'in' RIDs");

      final var outEl = db.lookupByRID(new RID(outStr), false);
      final var inEl = db.lookupByRID(new RID(inStr), false);
      final Vertex outV = outEl.asVertex(false);

      final boolean beganHere = !db.isTransactionActive();
      if (beganHere)
        db.begin();

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
    return CreateRecordResponse.newBuilder().setRid(d.getIdentity().toString()).build();
  }

  @Override
  public void lookupByRid(LookupByRidRequest req, StreamObserver<LookupByRidResponse> resp) {
    try {
      Database db = getDatabase(req.getDatabase(), req.getCredentials());

      final String ridStr = req.getRid();
      if (ridStr == null || ridStr.isBlank())
        throw new IllegalArgumentException("rid is required");

      var el = db.lookupByRID(new RID(ridStr), true);

      resp.onNext(LookupByRidResponse.newBuilder().setFound(true).setRecord(convertToGrpcRecord(el.getRecord(), db)).build());
      resp.onCompleted();
// CURRENT IMPL EXPECTS AN EXCEPTION INSTEAD
//    } catch (RecordNotFoundException e) {
//      resp.onNext(LookupByRidResponse.newBuilder().setFound(false).build());
//      resp.onCompleted();
    } catch (Exception e) {
      resp.onError(Status.INTERNAL.withDescription("LookupByRid: " + e.getMessage()).asException());
    }
  }

  @Override
  public void updateRecord(final UpdateRecordRequest req, final StreamObserver<UpdateRecordResponse> resp) {
    // Check for external transaction
    final String incomingTxId = req.hasTransaction() ? req.getTransaction().getTransactionId() : null;
    final TransactionContext txCtx = (incomingTxId != null && !incomingTxId.isBlank())
        ? activeTransactions.get(incomingTxId) : null;

    if (txCtx != null) {
      // External transaction — execute on its dedicated thread to maintain thread-local state
      try {
        final Future<UpdateRecordResponse> future = txCtx.executor.submit(() -> updateRecordInternal(req, txCtx.db));
        resp.onNext(future.get());
        resp.onCompleted();
      } catch (Exception e) {
        final Throwable cause = (e instanceof ExecutionException && e.getCause() != null) ? e.getCause() : e;
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

      // Lookup the record by RID
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

        // Exclude ArcadeDB system fields during update

        props.forEach((k, v) -> {
          String key = k.trim().toLowerCase();
          if (key.equals("@rid") || key.equals("@type") || key.equals("@cat")) {
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

        // Exclude ArcadeDB system fields during update

        props.forEach((k, v) -> {
          String key = k.trim().toLowerCase();
          if (key.equals("@rid") || key.equals("@type") || key.equals("@cat")) {
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
    final TransactionContext txCtx = (incomingTxId != null && !incomingTxId.isBlank())
        ? activeTransactions.get(incomingTxId) : null;

    if (txCtx != null) {
      // External transaction — execute on its dedicated thread to maintain thread-local state
      try {
        final Future<DeleteRecordResponse> future = txCtx.executor.submit(() -> deleteRecordInternal(req, txCtx.db));
        resp.onNext(future.get());
        resp.onCompleted();
      } catch (Exception e) {
        final Throwable cause = (e instanceof ExecutionException && e.getCause() != null) ? e.getCause() : e;
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
    try {
      ProjectionConfig projectionConfig = getProjectionConfig(request);

      LogManager.instance().log(this, Level.FINE, """
              executeQuery(): projectionConfig.include = %s projectionConfig\
              .mode = %s""",
          projectionConfig.isInclude(),
          projectionConfig.getEnc());

      // Force compression for streaming (usually beneficial)
      CompressionAwareService.setResponseCompression(responseObserver, "gzip");

      Database database = getDatabase(request.getDatabase(), request.getCredentials());

      // Check if this is part of a transaction
      if (request.hasTransaction()) {

        LogManager.instance().log(this, Level.FINE, "executeQuery(): has Tx %s",
            request.getTransaction().getTransactionId());

        TransactionContext txCtx = activeTransactions.get(request.getTransaction().getTransactionId());
        if (txCtx == null) {
          throw new IllegalArgumentException("Invalid transaction ID");
        }
        database = txCtx.db;
      }

      // Execute the query
      long startTime = System.currentTimeMillis();

      LogManager.instance().log(this, Level.FINE, "executeQuery(): query = %s", request.getQuery());

      ResultSet resultSet = database.query("sql", request.getQuery(),
          GrpcTypeConverter.convertParameters(request.getParametersMap()));

      LogManager.instance()
          .log(this, Level.FINE, "executeQuery(): to get resultSet = %s", (System.currentTimeMillis() - startTime));

      // Build response
      QueryResult.Builder resultBuilder = QueryResult.newBuilder();

      // Process results
      int count = 0;

      LogManager.instance().log(this, Level.FINE, "executeQuery(): resultSet.size = %s",
          resultSet.getExactSizeIfKnown());

      while (resultSet.hasNext()) {

        Result result = resultSet.next();

        LogManager.instance().log(this, Level.FINE, "executeQuery(): result = %s", result);

        // Convert Result to GrpcRecord, preserving aliases and all properties
        GrpcRecord grpcRecord = convertResultToGrpcRecord(result, database, projectionConfig);

        LogManager.instance().log(this, Level.FINE, "executeQuery(): grpcRecord -> @rid = %s", grpcRecord.getRid());

        resultBuilder.addRecords(grpcRecord);

        count++;

        // Apply limit if specified
        if (request.getLimit() > 0 && count >= request.getLimit()) {
          break;
        }
      }

      LogManager.instance().log(this, Level.FINE, "executeQuery(): count = %s", count);

      resultBuilder.setTotalRecordsInBatch(count);

      long executionTime = System.currentTimeMillis() - startTime;

      ExecuteQueryResponse response = ExecuteQueryResponse.newBuilder().addResults(resultBuilder.build())
          .setExecutionTimeMs(executionTime)
          .build();

      LogManager.instance().log(this, Level.FINE, "executeQuery(): executionTime + response generation = %s",
          executionTime);

      responseObserver.onNext(response);
      responseObserver.onCompleted();

    } catch (Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Error executing query: %s", e, e.getMessage());
      responseObserver.onError(Status.INTERNAL.withDescription("Query execution failed: " + e.getMessage()).asException());
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

  @Override
  public void beginTransaction(BeginTransactionRequest request,
                               StreamObserver<BeginTransactionResponse> responseObserver) {
    final String reqDb = request.getDatabase();
    final String user = (request.hasCredentials() ? request.getCredentials().getUsername() : null);

    LogManager.instance()
        .log(this, Level.FINE, "beginTransaction(): received request db=%s user=%s activeTxCount(before)=%s", reqDb,
            (user != null ? user : "<none>"),
            activeTransactions.size());

    // Declare txCtx outside try block so we can clean it up on failure
    TransactionContext txCtx = null;

    try {
      final Database database = getDatabase(reqDb, request.getCredentials());

      LogManager.instance().log(this, Level.FINE, """
              beginTransaction(): resolved database instance dbName=%s class=%s \
              hash=%s""",
          (database != null ? database.getName() : "<null>"), (database != null ?
              database.getClass().getSimpleName() : "<null>"),
          (database != null ? System.identityHashCode(database) : 0));

      // Generate transaction ID first so we can create the context
      final String transactionId = generateTransactionId();

      // Create transaction context with dedicated executor thread
      txCtx = new TransactionContext(database, transactionId);

      LogManager.instance().log(this, Level.FINE, """
          beginTransaction(): calling database.begin() on dedicated thread \
          for txId=%s""", transactionId);

      // Begin transaction ON THE DEDICATED THREAD - this is critical because ArcadeDB
      // transactions are thread-local
      Future<?> beginFuture = txCtx.executor.submit(() -> {
        // Initialize the DatabaseContext on this dedicated thread before any DB operation
        DatabaseContext.INSTANCE.init((DatabaseInternal) database);
        database.begin();
      });
      beginFuture.get(); // Wait for begin to complete

      // Register the transaction context
      activeTransactions.put(transactionId, txCtx);

      LogManager.instance()
          .log(this, Level.FINE, "beginTransaction(): started txId=%s for db=%s activeTxCount(after)=%s", transactionId,
              (database != null ? database.getName() : "<null>"), activeTransactions.size());

      final BeginTransactionResponse response = BeginTransactionResponse.newBuilder().setTransactionId(transactionId)
          .setTimestamp(System.currentTimeMillis()).build();

      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Throwable t) {
      // Shutdown the executor if it was created but not registered (prevents thread leak)
      if (txCtx != null) {
        txCtx.shutdown();
      }
      Throwable cause = (t instanceof ExecutionException && t.getCause() != null) ? t.getCause() : t;
      LogManager.instance()
          .log(this, Level.FINE, "beginTransaction(): FAILED db=%s user=%s err=%s", reqDb, (user != null ? user :
                  "<none>"),
              cause.toString(), cause);
      LogManager.instance().log(this, Level.SEVERE, "Error beginning transaction: %s", cause, cause.getMessage());
      responseObserver.onError(Status.INTERNAL.withDescription("Failed to begin transaction: " + cause.getMessage()).asException());
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

    // remove atomically to avoid double-commit races
    final TransactionContext txCtx = activeTransactions.remove(txId);

    LogManager.instance()
        .log(this, Level.FINE, "commitTransaction(): removed txId=%s, presentPreviously=%s, remainingActiveTx=%s", txId,
            txCtx != null,
            activeTransactions.size());

    if (txCtx == null) {
      // Idempotent no-op
      LogManager.instance()
          .log(this, Level.FINE, "commitTransaction(): no active tx for id=%s, responding committed=false", txId);
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
      Throwable cause = (t instanceof ExecutionException && t.getCause() != null) ? t.getCause() : t;
      LogManager.instance().log(this, Level.FINE, "commitTransaction(): commit FAILED txId=%s err=%s", txId,
          cause.toString(), cause);
      // tx is unusable; do not reinsert into the map
      rsp.onError(Status.ABORTED.withDescription("Commit failed: " + cause.getMessage()).asException());
    } finally {
      // Always shutdown the executor
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

    final TransactionContext txCtx = activeTransactions.remove(txId);

    LogManager.instance()
        .log(this, Level.FINE, "rollbackTransaction(): removed txId=%s, presentPreviously=%s, remainingActiveTx=%s",
            txId,
            txCtx != null,
            activeTransactions.size());

    if (txCtx == null) {
      LogManager.instance()
          .log(this, Level.FINE, "rollbackTransaction(): no active tx for id=%s, responding rolledBack=false", txId);
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
      Throwable cause = (t instanceof ExecutionException && t.getCause() != null) ? t.getCause() : t;
      LogManager.instance().log(this, Level.FINE, "rollbackTransaction(): rollback FAILED txId=%s err=%s", txId,
          cause.toString(), cause);
      rsp.onError(Status.ABORTED.withDescription("Rollback failed: " + cause.getMessage()).asException());
    } finally {
      // Always shutdown the executor
      txCtx.shutdown();
    }
  }

  @Override
  public void streamQuery(StreamQueryRequest request, StreamObserver<QueryResult> responseObserver) {
    ProjectionConfig projectionConfig = getProjectionConfigFromRequest(request);

    final ServerCallStreamObserver<QueryResult> scso = (ServerCallStreamObserver<QueryResult>) responseObserver;

    final AtomicBoolean cancelled = new AtomicBoolean(false);
    scso.setOnCancelHandler(() -> cancelled.set(true));

    Database db = null;
    boolean beganHere = false;

    try {
      db = getDatabase(request.getDatabase(), request.getCredentials());
      final int batchSize = Math.max(1, request.getBatchSize());

      // --- TX begin if requested ---
      final boolean hasTx = request.hasTransaction();
      final var tx = hasTx ? request.getTransaction() : null;
      if (hasTx && tx.getBegin()) {
        db.begin();
        beganHere = true;
      }

      // --- Dispatch on mode (helpers do NOT manage transactions) ---
      switch (request.getRetrievalMode()) {
        case MATERIALIZE_ALL -> streamMaterialized(db, request, batchSize, scso, cancelled, projectionConfig);
        case PAGED -> streamPaged(db, request, batchSize, scso, cancelled, projectionConfig);
        case CURSOR -> streamCursor(db, request, batchSize, scso, cancelled, projectionConfig);
        default -> streamCursor(db, request, batchSize, scso, cancelled, projectionConfig);
      }

      // If the client cancelled mid-stream, choose rollback unless caller explicitly
      // asked to commit/rollback.
      if (cancelled.get()) {
        if (hasTx) {
          if (tx.getRollback()) {
            db.rollback();
          } else if (tx.getCommit()) {
            db.commit(); // caller explicitly wanted commit even if they cancelled
          } else if (beganHere) {
            db.rollback(); // safe default on cancellation
          }
        }
        return; // don't call onCompleted()
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
        responseObserver.onError(Status.INTERNAL.withDescription("Stream query failed: " + e.getMessage()).asException());
      }
    }
  }

  /**
   * Mode 1 (existing behavior-ish): run once and iterate results, batching as we
   * go.
   */
  private void streamCursor(Database db, StreamQueryRequest request, int batchSize,
                            ServerCallStreamObserver<QueryResult> scso,
                            AtomicBoolean cancelled, ProjectionConfig projectionConfig) {

    long running = 0L;

    QueryResult.Builder batch = QueryResult.newBuilder();
    int inBatch = 0;

    try (ResultSet rs = db.query("sql", request.getQuery(),
        GrpcTypeConverter.convertParameters(request.getParametersMap()))) {

      while (rs.hasNext()) {

        if (cancelled.get())
          return;
        waitUntilReady(scso, cancelled);

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
                                  AtomicBoolean cancelled, ProjectionConfig projectionConfig) {

    final List<GrpcRecord> all = new ArrayList<>();

    try (ResultSet rs = db.query("sql", request.getQuery(),
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
      }
    }

    long running = 0L;
    for (int i = 0; i < all.size(); i += batchSize) {
      if (cancelled.get())
        return;
      waitUntilReady(scso, cancelled);

      int end = Math.min(i + batchSize, all.size());
      QueryResult.Builder b = QueryResult.newBuilder();
      for (int j = i; j < end; j++)
        b.addRecords(all.get(j));

      running += (end - i);
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
                           AtomicBoolean cancelled, ProjectionConfig projectionConfig) {

    final String pagedSql = wrapWithSkipLimit(request.getQuery()); // see helper below
    int offset = 0;
    long running = 0L;

    while (true) {
      if (cancelled.get())
        return;
      waitUntilReady(scso, cancelled);

      Map<String, Object> params = new HashMap<>(GrpcTypeConverter.convertParameters(request.getParametersMap()));
      params.put("_skip", offset);
      params.put("_limit", batchSize);

      int count = 0;
      QueryResult.Builder b = QueryResult.newBuilder();

      try (ResultSet rs = db.query("sql", pagedSql, params)) {
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

  private void waitUntilReady(ServerCallStreamObserver<?> scso, AtomicBoolean cancelled) {
    // Skip if you're okay with best-effort pushes; otherwise honor transport
    // readiness
    if (scso.isReady())
      return;
    while (!scso.isReady()) {
      if (cancelled.get())
        return;
      // avoid burning CPU:
      try {
        Thread.sleep(1);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        return;
      }
    }
  }

  // --- 1) Unary bulk ---
  @Override
  public void bulkInsert(BulkInsertRequest req, StreamObserver<InsertSummary> resp) {

    final InsertOptions opts = defaults(req.getOptions()); // apply defaults (batch size, tx mode, etc.)
    final long started = System.currentTimeMillis();

    try (InsertContext ctx = new InsertContext(opts)) {

      Counts totals = insertRows(ctx, req.getRowsList().iterator());

      ctx.flushCommit(true);

      resp.onNext(ctx.summary(totals, started));
      resp.onCompleted();
    } catch (Exception e) {
      resp.onError(Status.INTERNAL.withDescription("bulkInsert: " + e.getMessage()).asException());
    }
  }

  // --- 2) Client-streaming; single summary at end ---
  @Override
  public StreamObserver<InsertChunk> insertStream(StreamObserver<InsertSummary> resp) {
    final ServerCallStreamObserver<InsertSummary> call = (ServerCallStreamObserver<InsertSummary>) resp;

    call.disableAutoInboundFlowControl();

    final long startedAt = System.currentTimeMillis();

    final AtomicBoolean cancelled = new AtomicBoolean(false);
    final AtomicReference<InsertContext> ctxRef = new AtomicReference<>();

    final Counts totals = new Counts();

    // cache the first-chunk effective options to validate consistency
    final AtomicReference<InsertOptions> firstOptsRef = new AtomicReference<>();

    call.setOnCancelHandler(() -> cancelled.set(true));

    // Pull the very first inbound message
    call.request(1);

    return new StreamObserver<>() {

      @Override
      public void onNext(InsertChunk c) {

        if (cancelled.get()) {
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
          // Register as an error on totals; keep stream alive unless you prefer to fail
          // the call
          totals.err(Math.max(0, (ctxRef.get() != null ? ctxRef.get().received : 0) - 1), "DB_ERROR", e.getMessage(),
              "");
        } finally {
          if (!cancelled.get())
            call.request(1);
        }
      }

      @Override
      public void onError(Throwable t) {
        InsertContext ctx = ctxRef.get();
        if (ctx != null)
          ctx.closeQuietly();
      }

      @Override
      public void onCompleted() {

        try {

          InsertContext ctx = ctxRef.get();

          if (ctx == null) {
            // Client closed without sending a first chunk → nothing to do
            resp.onNext(InsertSummary.newBuilder().setReceived(0).setInserted(0).setUpdated(0).setIgnored(0).setFailed(0)
                .setExecutionTimeMs(System.currentTimeMillis() - startedAt).build());

            resp.onCompleted();
            return;
          }

          ctx.flushCommit(true); // commit if not validate-only

          if (!cancelled.get()) {
            resp.onNext(ctx.summary(totals, startedAt));
            resp.onCompleted();
          }
        } catch (Exception e) {
          resp.onError(Status.INTERNAL.withDescription("insertStream: " + e.getMessage()).asException());
        } finally {
          InsertContext ctx = ctxRef.get();
          if (ctx != null)
            ctx.closeQuietly();
        }
      }
    };
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

    final AtomicReference<InsertContext> ref = new AtomicReference<>();
    final AtomicBoolean cancelled = new AtomicBoolean(false);
    final AtomicBoolean started = new AtomicBoolean(false);

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
    final Consumer<InsertResponse> sendResponse = resp::onNext;

    // Cleanup helper that can be called multiple times safely
    final Runnable cleanupAndShutdown = () -> {
      cancelled.set(true);
      try {
        streamExecutor.submit(() -> {
          final InsertContext ctx = ref.getAndSet(null);
          if (ctx != null) {
            sessionWatermark.remove(ctx.sessionId);
            ctx.closeQuietly();
          }
        });
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

        // Submit all message processing to the single-threaded executor
        // to ensure transaction ThreadLocal context is preserved
        streamExecutor.submit(() -> {
          if (cancelled.get())
            return;

          try {
            switch (reqMsg.getMsgCase()) {

              case START -> {
                if (!started.compareAndSet(false, true)) {
                  resp.onError(
                      Status.FAILED_PRECONDITION.withDescription("insertBidirectional: START already received").asException());
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

                  resp.onNext(InsertResponse.newBuilder().setBatchAck(BatchAck.newBuilder().setSessionId(ctx.sessionId)
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
                  resp.onNext(InsertResponse.newBuilder().setCommitted(Committed.newBuilder().setSummary(sum).build()).build());
                  resp.onCompleted();
                } catch (Exception e) {
                  resp.onError(Status.INTERNAL.withDescription("commit: " + e.getMessage()).asException());
                } finally {
                  sessionWatermark.remove(ctx.sessionId);
                  ctx.closeQuietly();
                  ref.set(null);
                }
              }

              case MSG_NOT_SET -> {
                // ignore
                call.request(1);
              }
            }
          } catch (Exception unexpected) {
            // defensive: fail fast on unexpected exceptions
            resp.onError(Status.INTERNAL.withDescription("insertBidirectional: " + unexpected.getMessage()).asException());
            final InsertContext ctx = ref.getAndSet(null);
            if (ctx != null) {
              sessionWatermark.remove(ctx.sessionId);
              ctx.closeQuietly();
            }
          }
        });
      }

      @Override
      public void onError(Throwable t) {
        cleanupAndShutdown.run();
      }

      @Override
      public void onCompleted() {
        // Define your policy for half-close without COMMIT:
        try {
          streamExecutor.submit(() -> {
            final InsertContext ctx = ref.getAndSet(null);
            if (ctx != null) {
              try {
                // Safer default is rollback; if you prefer auto-commit on close, call
                // flushCommit(true)
                ctx.flushCommit(false); // or ctx.rollbackQuietly() if you have a helper
              } catch (Exception ignore) {
              }
              sessionWatermark.remove(ctx.sessionId);
              ctx.closeQuietly();
            }
          });
        } catch (RejectedExecutionException ignore) {
          // Executor already shut down
        }
        streamExecutor.shutdown();
      }
    };
  }

  private boolean tryUpsertVertex(final InsertContext ctx, final MutableVertex incoming) {

    var keys = ctx.keyCols;

    if (keys.isEmpty())
      return false;

    // Read incoming values via the (read-only) document view
    var inDoc = incoming.asDocument();

    String where = String.join(" AND ", keys.stream().map(k -> k + " = ?").toList());
    Object[] params = keys.stream().map(inDoc::get).toArray();

    // Prefer selecting the element so we can get a mutable vertex directly
    final String sql = "SELECT FROM " + ctx.opts.getTargetClass() + " WHERE " + where;

    try (var rs = ctx.db.query("sql", sql, params)) {
      if (!rs.hasNext())
        return false;

      var res = rs.next();
      if (!res.isElement())
        return false;

      Vertex v = res.getElement().get().asVertex();

      MutableVertex existingV = v.modify();

      // Apply the updates

      for (String col : ctx.updateCols) {
        existingV.set(col, inDoc.get(col));
      }

      existingV.save();

      return true;
    }
  }

  private boolean tryUpsertDocument(InsertContext ctx, MutableDocument incoming) {

    var keys = ctx.keyCols;

    if (keys.isEmpty())
      return false;

    String where = String.join(" AND ", keys.stream().map(k -> k + " = ?").toList());
    Object[] params = keys.stream().map(incoming::get).toArray();

    try (ResultSet rs = ctx.db.query("sql", "SELECT FROM " + ctx.opts.getTargetClass() + " WHERE " + where, params)) {

      if (!rs.hasNext()) {
        return false;
      }

      var res = rs.next();
      if (!res.isElement())
        return false;

      Document d = res.getElement().get().asDocument();

      var existing = d.modify();

      // Apply the updates

      for (String col : ctx.updateCols) {
        existing.set(col, incoming.get(col));
      }

      existing.save();

      return true;
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
    boolean isVertex = dt instanceof VertexType;
    boolean isEdge = dt instanceof EdgeType;

    while (it.hasNext()) {

      GrpcRecord r = it.next();

      c.received++;
      ctx.received++;

      try {
        if (ctx.opts.getValidateOnly())
          continue;

        if (isVertex) {
          MutableVertex v = ctx.db.newVertex(ctx.opts.getTargetClass());
          applyGrpcRecord(v, r);
          if (ctx.opts.getConflictMode() == ConflictMode.CONFLICT_UPDATE && tryUpsertVertex(ctx, v)) {
            c.updated++;
          } else {
            v.save();
            c.inserted++;
          }
        } else if (isEdge) {

          String outRid = getStringProp(r, "out"); // lookup helper you already have
          String inRid = getStringProp(r, "in");

          if (outRid == null || inRid == null) {

            c.failed++;

            c.errors.add(InsertError.newBuilder().setRowIndex(ctx.received - 1).setCode("MISSING_ENDPOINTS")
                .setMessage("Edge requires 'out' and 'in'").build());
          } else {
            var outV = ctx.db.lookupByRID(new RID(outRid), false).asVertex(false);

            // Create edge from the OUT vertex
            MutableEdge e = outV.newEdge(ctx.opts.getTargetClass(), new RID(inRid));
            applyGrpcRecord(e, r); // sets edge properties
            e.save();
            c.inserted++;
          }
        } else {
          MutableDocument d = ctx.db.newDocument(ctx.opts.getTargetClass());
          applyGrpcRecord(d, r);
          if (ctx.opts.getConflictMode() == ConflictMode.CONFLICT_UPDATE && tryUpsertDocument(ctx, d)) {
            c.updated++;
          } else {
            d.save();
            c.inserted++;
          }
        }

      } catch (DuplicatedKeyException dup) {
        switch (ctx.opts.getConflictMode()) {
          case CONFLICT_IGNORE -> c.ignored++;
          case CONFLICT_ABORT, UNRECOGNIZED -> c.err(ctx.received - 1, "CONFLICT", dup.getMessage(), "");
          case CONFLICT_UPDATE -> c.err(ctx.received - 1, "CONFLICT", dup.getMessage(), "");
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
      case TIMESTAMP_VALUE:
        return dbgDec("fromGrpcValue", v, new Date(GrpcTypeConverter.tsToMillis(v.getTimestampValue())), null);
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
        (o == null ? "null" : o.getClass().getName()));

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

      final boolean inProjection = (pc != null && pc.include);

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
            (doc.getIdentity() != null ? doc.getIdentity() : "null"), (doc.getType() != null ? doc.getTypeName() :
                "null"));
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
            (doc.getIdentity() != null ? doc.getIdentity() : "null"), (doc.getType() != null ? doc.getTypeName() :
                "null"));

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
              (doc.getIdentity() != null ? doc.getIdentity() : "null"), (doc.getType() != null ? doc.getTypeName() :
                  "null"),
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

    long startedAt;

    final String sessionId = UUID.randomUUID().toString();
    long received = 0;

    InsertContext(InsertOptions opts) {

      this.opts = opts;

      this.db = getDatabase(opts.getDatabase(), opts.getCredentials());

      this.keyCols = opts.getKeyColumnsList();

      this.updateCols = opts.getUpdateColumnsOnConflictList();

      if (!opts.getValidateOnly()) {
        if (opts.getTransactionMode() == TransactionMode.PER_STREAM || opts.getTransactionMode() == TransactionMode.PER_BATCH
            || opts.getTransactionMode() == TransactionMode.PER_REQUEST || opts.getTransactionMode() == TransactionMode.PER_ROW) {
          db.begin();
        }
      }
    }

    void flushCommit(boolean end) {

      if (opts.getValidateOnly()) {
        if (end)
          db.rollback();
        return;
      }
      switch (opts.getTransactionMode()) {
        case PER_ROW -> {
          db.commit();
          if (!end)
            db.begin();
        }
        case PER_REQUEST -> {
          if (end)
            db.commit();
        }
        case PER_BATCH -> {
          db.commit();
          if (!end)
            db.begin();
        }
        case PER_STREAM -> {
          if (end)
            db.commit();
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

    @Override
    public void close() {
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

    // Validate credentials
    validateCredentials(credentials);

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
        DatabaseContext.INSTANCE.init((DatabaseInternal) db);
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

  private void validateCredentials(DatabaseCredentials credentials) {
    // Check if user is already authenticated via the interceptor (e.g., via Bearer token)
    final String authenticatedUser = GrpcAuthInterceptor.USER_CONTEXT_KEY.get();
    if (authenticatedUser != null && !authenticatedUser.isEmpty()) {
      // User already authenticated via interceptor, no need to validate credentials
      LogManager.instance().log(this, Level.FINE, """
          validateCredentials(): user already authenticated via interceptor:\
           %s""", authenticatedUser);
      return;
    }

    // Implement credential validation logic
    // This is a placeholder - integrate with ArcadeDB's security system
    if (credentials == null || credentials.getUsername().isEmpty()) {
      throw new IllegalArgumentException("Invalid credentials");
    }
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

    // Iterate over ALL properties from the Result, including aliases
    for (String propertyName : result.getPropertyNames()) {
      Object value = result.getProperty(propertyName);

      if (value != null) {
        LogManager.instance()
            .log(this, Level.FINE, "convertResultToGrpcRecord(): Converting %s\n  value = %s\n  class = %s",
                propertyName, value, value.getClass());

        GrpcValue gv = projectionConfig != null ?
            toGrpcValue(value, projectionConfig) :
            toGrpcValue(value);

        LogManager.instance()
            .log(this, Level.FINE, "ENC-RES %s: %s -> %s", propertyName, summarizeJava(value), summarizeGrpc(gv));

        builder.putProperties(propertyName, gv);
      }
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

          LogManager.instance()
              .log(this, Level.FINE, "convertToGrpcRecord(): Converting %s\n  value = %s\n  class = %s", propertyName
                  , value,
                  value.getClass());

          GrpcValue gv = toGrpcValue(value);

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

    Object propValue = result.getProperty(propName);

    LogManager.instance()
        .log(this, Level.FINE, "convertPropToGrpcValue(): Converting %s\n  value = %s\n  class = %s", propName,
            propValue,
            propValue.getClass());

    return toGrpcValue(propValue, pc);
  }

  private GrpcValue convertPropToGrpcValue(String propName, Result result) {

    Object propValue = result.getProperty(propName);

    LogManager.instance()
        .log(this, Level.FINE, "convertPropToGrpcValue(): Converting %s\n  value = %s\n  class = %s", propName,
            propValue,
            propValue.getClass());

    return toGrpcValue(propValue);
  }

  private Object toJavaForProperty(final Database db, final MutableDocument parent, final DocumentType dtype,
                                   final String propName,
                                   final GrpcValue grpcValue) {

    if (grpcValue == null)
      return null;

    // Try schema
    Property prop = null;
    try {
      prop = (dtype != null) ? dtype.getProperty(propName) : null;
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
        // Prefer timestamp_value; else accept epoch ms in int64; else parse string
        return switch (v.getKindCase()) {
          case TIMESTAMP_VALUE -> new Date(GrpcTypeConverter.tsToMillis(v.getTimestampValue()));
          case INT64_VALUE -> new Date(v.getInt64Value());
          case STRING_VALUE -> new Date(Long.parseLong(v.getStringValue())); // or parse ISO if you emit it
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
            (v.getKindCase() == GrpcValue.KindCase.EMBEDDED_VALUE && !v.getEmbeddedValue().getType().isEmpty())
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
            (v.getKindCase() == GrpcValue.KindCase.EMBEDDED_VALUE && !v.getEmbeddedValue().getType().isEmpty())
                ? "payload"
                : "schema/discovered";
        int fields = (v.getKindCase() == GrpcValue.KindCase.EMBEDDED_VALUE) ? v.getEmbeddedValue().getFieldsCount()
            : (v.getKindCase() == GrpcValue.KindCase.MAP_VALUE) ? v.getMapValue().getEntriesCount() : 0;
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
              (embeddedType != null ? "FOUND" : "NOT FOUND"));
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
              Object j = (embeddedTypeFinal != null) ? toJavaForProperty(db, ed, embeddedTypeFinal, k, vv) :
                  fromGrpcValue(vv);
              LogManager.instance().log(this, Level.FINE, "EMBEDDED: field '%s' converted -> %s", k,
                  (j == null ? "null" : j.getClass().getSimpleName()));
              ed.set(k, j);
            });
            break;

          case MAP_VALUE:
            v.getMapValue().getEntriesMap().forEach((k, vv) -> {
              LogManager.instance().log(this, Level.FINE, "EMBEDDED: field '%s' raw kind=%s", k, vv.getKindCase());
              Object j = (embeddedTypeFinal != null) ? toJavaForProperty(db, ed, embeddedTypeFinal, k, vv) :
                  fromGrpcValue(vv);
              LogManager.instance().log(this, Level.FINE, "EMBEDDED: field '%s' converted -> %s", k,
                  (j == null ? "null" : j.getClass().getSimpleName()));
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
        // Same handling as DATETIME
        return switch (v.getKindCase()) {
          case TIMESTAMP_VALUE -> new Date(GrpcTypeConverter.tsToMillis(v.getTimestampValue()));
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
    return "tx_" + System.nanoTime();
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
    LogManager.instance()
        .log(this, Level.FINE, "GRPC-ENC [%s]%s in=%s -> out=%s", where, (ctx == null ? "" : " " + ctx),
            summarizeJava(in),
            summarizeGrpc(out));
    return out;
  }

  private Object dbgDec(String where, GrpcValue in, Object out, String ctx) {
    LogManager.instance()
        .log(this, Level.FINE, "GRPC-DEC [%s]%s in=%s -> out=%s", where, (ctx == null ? "" : " " + ctx),
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
