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
package com.arcadedb.remote.grpc;

import com.arcadedb.engine.timeseries.AggregationType;
import com.arcadedb.remote.RemoteException;
import com.arcadedb.remote.timeseries.TimeSeriesBucket;
import com.arcadedb.remote.timeseries.TimeSeriesLatestResult;
import com.arcadedb.remote.timeseries.TimeSeriesPoint;
import com.arcadedb.remote.timeseries.TimeSeriesQuery;
import com.arcadedb.remote.timeseries.TimeSeriesQueryResult;
import com.arcadedb.remote.timeseries.TimeSeriesWriteSummary;
import com.arcadedb.server.grpc.TimeSeriesAggregation;
import com.arcadedb.server.grpc.TimeSeriesAggregationRequest;
import com.arcadedb.server.grpc.TimeSeriesAggregationType;
import com.arcadedb.server.grpc.TimeSeriesLatestRequest;
import com.arcadedb.server.grpc.TimeSeriesLatestResponse;
import com.arcadedb.server.grpc.TimeSeriesPrecision;
import com.arcadedb.server.grpc.TimeSeriesQueryRequest;
import com.arcadedb.server.grpc.TimeSeriesRow;
import com.arcadedb.server.grpc.TimeSeriesTagFilter;
import com.arcadedb.server.grpc.TimeSeriesWriteChunk;
import com.arcadedb.server.grpc.TimeSeriesWriteRequest;
import com.arcadedb.ContextConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.executor.InternalResultSet;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.remote.RemoteException;
import com.arcadedb.remote.RemoteImmutableDocument;
import com.arcadedb.remote.RemoteImmutableEdge;
import com.arcadedb.remote.RemoteImmutableVertex;
import com.arcadedb.remote.RemoteSchema;
import com.arcadedb.remote.RemoteTransactionExplicitLock;
import com.arcadedb.remote.grpc.utils.ProtoUtils;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.VertexType;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.grpc.ArcadeDbServiceGrpc;
import com.arcadedb.server.grpc.BatchAck;
import com.arcadedb.server.grpc.BeginTransactionRequest;
import com.arcadedb.server.grpc.BeginTransactionResponse;
import com.arcadedb.server.grpc.BulkInsertRequest;
import com.arcadedb.server.grpc.Commit;
import com.arcadedb.server.grpc.CommitTransactionRequest;
import com.arcadedb.server.grpc.CommitTransactionResponse;
import com.arcadedb.server.grpc.CreateRecordRequest;
import com.arcadedb.server.grpc.CreateRecordResponse;
import com.arcadedb.server.grpc.DatabaseCredentials;
import com.arcadedb.server.grpc.DeleteRecordRequest;
import com.arcadedb.server.grpc.DeleteRecordResponse;
import com.arcadedb.server.grpc.ExecuteCommandRequest;
import com.arcadedb.server.grpc.ExecuteCommandResponse;
import com.arcadedb.server.grpc.ExecuteQueryRequest;
import com.arcadedb.server.grpc.ExecuteQueryResponse;
import com.arcadedb.server.grpc.FullTextSearchRequest;
import com.arcadedb.server.grpc.FullTextSearchResponse;
import com.arcadedb.server.grpc.GetProgressRequest;
import com.arcadedb.server.grpc.GetProgressResponse;
import com.arcadedb.server.grpc.GrpcRecord;
import com.arcadedb.server.grpc.GrpcValue;
import com.arcadedb.server.grpc.HybridSearchRequest;
import com.arcadedb.server.grpc.HybridSearchResponse;
import com.arcadedb.server.grpc.InsertChunk;
import com.arcadedb.server.grpc.InsertOptions;
import com.arcadedb.server.grpc.InsertRequest;
import com.arcadedb.server.grpc.InsertResponse;
import com.arcadedb.server.grpc.InsertSummary;
import com.arcadedb.server.grpc.LookupByRidRequest;
import com.arcadedb.server.grpc.LookupByRidResponse;
import com.arcadedb.server.grpc.OperationProgressInfo;
import com.arcadedb.server.grpc.ProjectionSettings.ProjectionEncoding;
import com.arcadedb.server.grpc.ProjectionSettings;
import com.arcadedb.server.grpc.PropertiesUpdate;
import com.arcadedb.server.grpc.QueryResult;
import com.arcadedb.server.grpc.RollbackTransactionRequest;
import com.arcadedb.server.grpc.RollbackTransactionResponse;
import com.arcadedb.server.grpc.Start;
import com.arcadedb.server.grpc.StreamQueryRequest;
import com.arcadedb.server.grpc.TransactionContext;
import com.arcadedb.server.grpc.TransactionIsolation;
import com.arcadedb.server.grpc.UpdateRecordRequest;
import com.arcadedb.server.grpc.UpdateRecordResponse;
import com.arcadedb.server.grpc.VectorSearchRequest;
import com.arcadedb.server.grpc.VectorSearchResponse;
import com.google.protobuf.Int32Value;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.BlockingClientCall;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.StreamObserver;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.stream.Collectors;

/**
 * Remote Database implementation using gRPC protocol instead of HTTP. Extends
 * RemoteDatabase to maintain compatibility while overriding network operations.
 * It's not thread safe. For multi-thread usage create one instance of
 * RemoteGrpcDatabase per thread.
 *
 * @author Oleg Cohen (oleg.cohen@gmail.com)
 */
public class RemoteGrpcDatabase extends RemoteDatabase {

  private final    ArcadeDbServiceGrpc.ArcadeDbServiceBlockingV2Stub blockingStub;
  private final    ArcadeDbServiceGrpc.ArcadeDbServiceStub           asyncStub;
  private final    RemoteSchema                                      schema;
  private final    String                                            userName;
  private final    String                                            userPassword;
  private final    String                                            databaseName;
  private          String                                            transactionId;
  private          RemoteTransactionExplicitLock                     explicitLock;
  protected        RemoteGrpcServer                                  remoteGrpcServer;
  // ---- fields ----
  private volatile TxDebug                                           debugTx;

  public RemoteGrpcDatabase(final RemoteGrpcServer remoteGrpcServer, final String server, final int grpcPort,
                            final int httpPort,
                            final String databaseName, final String userName, final String userPassword) {
    this(remoteGrpcServer, server, grpcPort, httpPort, databaseName, userName, userPassword,
        new ContextConfiguration());
  }

  public RemoteGrpcDatabase(final RemoteGrpcServer remoteGrpcServer, final String host, final int grpcPort,
                            final int httpPort,
                            final String databaseName, final String userName, final String userPassword,
                            final ContextConfiguration configuration) {
    super(host, httpPort, databaseName, userName, userPassword, configuration);
    this.remoteGrpcServer = remoteGrpcServer;
    this.userName = userName;
    this.userPassword = userPassword;
    this.databaseName = databaseName;
    this.blockingStub = createBlockingStub();
    this.asyncStub = createAsyncStub();
    this.schema = new RemoteSchema(this);
  }

  /**
   * Override this method to customize blocking stub creation.
   * <p>
   * The stub names this database on every call, so the server's auth interceptor authenticates the
   * caller against the database the calls actually target. Before #7320 no database travelled with the
   * call and the server fell back to the literal name {@code "default"}, which refused every principal
   * whose grants named real databases.
   */
  protected ArcadeDbServiceGrpc.ArcadeDbServiceBlockingV2Stub createBlockingStub() {
    return this.remoteGrpcServer.newBlockingStub(getTimeout(), databaseName);
  }

  /**
   * @see #createBlockingStub()
   */
  protected ArcadeDbServiceGrpc.ArcadeDbServiceStub createAsyncStub() {
    return this.remoteGrpcServer.newAsyncStub(getTimeout(), databaseName);
  }

  @Override
  public String getDatabasePath() {
    return "grpc://" + this.remoteGrpcServer.channel().authority() + "/" + getName();
  }

  @Override
  public RemoteSchema getSchema() {
    return schema;
  }

  /**
   * {@inheritDoc}
   * <p>
   * Overridden to poll over gRPC. The inherited {@code RemoteDatabase.getProgress()} issues
   * {@code GET /api/v1/progress/{database}} against the HTTP port - the port a gRPC-only deployment does
   * not open - so on such a server the inherited method could only fail to connect (issue #7310). Nothing
   * about it said so: it compiled, it existed, and it asked for a listener that was not there.
   * <p>
   * The returned documents carry the same keys the HTTP route emits, so a caller already reading
   * {@code RemoteDatabase.getProgress()} needs no change to run over gRPC.
   * <p>
   * The request carries THIS database's credentials, not the {@link RemoteGrpcServer}'s: the two are
   * constructed separately and a database opened by a scoped user may share a channel with a server built
   * for another account.
   */
  @Override
  public List<JSONObject> getProgress() {
    checkDatabaseIsOpen();

    final GetProgressRequest request = GetProgressRequest.newBuilder().setCredentials(buildCredentials())
        .setDatabase(getName()).build();

    try {
      // The stub is built per call, not cached: withDeadlineAfter fixes an ABSOLUTE deadline the moment it
      // is applied, so a stub held across calls would carry a deadline that has already passed by the second
      // poll. Building it also re-resolves the channel, so a server restarted through start() is honoured.
      // The cost is a few field copies on a shared channel, which is nothing next to the round trip.
      final GetProgressResponse response = callUnary("GetProgress",
          () -> remoteGrpcServer.newAdminBlockingStub(getTimeout()).getProgress(request));

      final List<JSONObject> operations = new ArrayList<>(response.getOperationsCount());
      for (final OperationProgressInfo info : response.getOperationsList())
        operations.add(toProgressJson(info));
      return operations;
    } catch (final StatusRuntimeException | StatusException e) {
      handleGrpcException(e); // rethrows mapped domain exception
      throw new IllegalStateException("unreachable");
    }
  }

  /**
   * One progress entry as the JSON document {@code RemoteDatabase.getProgress()} contracts to return -
   * the same keys, in the same units, that {@code OperationProgress.toJSON()} writes on the HTTP route.
   */
  private static JSONObject toProgressJson(final OperationProgressInfo info) {
    return new JSONObject()
        .put("id", info.getId())
        .put("database", info.getDatabase())
        .put("operation", info.getOperation())
        .put("stepName", info.getStepName())
        .put("stepIndex", info.getStepIndex())
        .put("totalSteps", info.getTotalSteps())
        .put("done", info.getDone())
        .put("total", info.getTotal())
        .put("percentage", info.getPercentage())
        .put("startedOn", info.getStartedOn())
        .put("elapsedMs", info.getElapsedMs());
  }

  @Override
  public void close() {

    if (transactionId != null) {
      rollback();
    }
    super.close();
  }

  @Override
  public void begin(final Database.TRANSACTION_ISOLATION_LEVEL isolationLevel) {
    checkDatabaseIsOpen();
    if (transactionId != null)
      throw new TransactionException("Transaction already begun");

    txCreatedRecords.clear();

    BeginTransactionRequest request =
        BeginTransactionRequest.newBuilder().setDatabase(getName()).setCredentials(buildCredentials())
            .setIsolation(mapIsolationLevel(isolationLevel)).build();

    callUnaryVoid("BeginTransaction", () -> {

      try {
        BeginTransactionResponse response = blockingStub.withDeadlineAfter(getTimeout(), TimeUnit.MILLISECONDS)
            .beginTransaction(request);
        transactionId = response.getTransactionId();
        // Store transaction ID in parent class session management
        setSessionId(transactionId);
      } catch (StatusRuntimeException | StatusException e) {
        throw new TransactionException("Error on transaction begin", e);
      }

      debugTx = new TxDebug(this.getName(), /* label set by TM */ null);
      logTx("BEGIN(local)", null);
      try {
        checkCrossThreadUse("before BeginTransaction");
        // send BeginTransaction RPC ...
        debugTx.beginRpcSent = true;
        debugTx.rpcSeq.incrementAndGet();
        logTx("BEGIN sent", "BeginTransaction");
      } catch (RuntimeException e) {
        logTx("BEGIN failed", "BeginTransaction");
        debugTx = null;
        throw e;
      }
    });
  }

  @Override
  public void commit() {

    checkDatabaseIsOpen();
    stats.writeTx.incrementAndGet();

    if (transactionId == null) {
      throw new TransactionException("Transaction not begun");
    }

    logTx("COMMIT(local)", null);
    checkCrossThreadUse("before CommitTransaction");

    boolean committed = false;
    try {
      callUnaryVoid("CommitTransaction", () -> {

      try {

        CommitTransactionRequest request = CommitTransactionRequest.newBuilder()
            .setTransaction(TransactionContext.newBuilder().setTransactionId(transactionId).setDatabase(getName()).build())
            .setCredentials(buildCredentials())
            .build();

        try {

          CommitTransactionResponse response = blockingStub.withDeadlineAfter(getTimeout(), TimeUnit.MILLISECONDS)
              .commitTransaction(request);

          LogManager.instance()
              .log(this, Level.FINE, "[After commit] Success: %s Committed: %s", response.getSuccess(),
                  response.getCommitted());

          if (!response.getSuccess()) {
            throw new TransactionException("Failed to commit transaction: " + response.getMessage());
          }
          // success=true only means the RPC completed; committed=false means the server did NOT durably
          // commit (e.g. the transaction was already rolled back by the idle reaper). Surfacing this as a
          // failure prevents silent data loss where the client believes its writes are durable.
          if (!response.getCommitted()) {
            throw new TransactionException("Transaction was not committed on the server: " + response.getMessage());
          }
        } catch (StatusRuntimeException | StatusException e) {
          handleGrpcException(e);
        } finally {
          transactionId = null;
          setSessionId(null);
        }

        if (debugTx != null) {
          debugTx.committed = true;
          debugTx.rpcSeq.incrementAndGet();
        }

        logTx("COMMIT sent", "CommitTransaction");
      } finally {
        debugTx = null;
      }

      });
      committed = true;
    } finally {
      if (committed)
        // SUCCESSFUL COMMIT: THE ASSIGNED RIDs ARE DURABLE, JUST DROP THE TRACKING
        txCreatedRecords.clear();
      else
        // FAILED COMMIT = SERVER-SIDE ROLLBACK: RESET CREATED RECORDS SO THEY CAN BE CLEANLY RE-INSERTED (ISSUE #4562)
        resetCreatedRecordsIdentity();
    }
  }

  /**
   * Rolls back the active transaction on the server.
   *
   * <p><b>Behavior change (issue #5042):</b> if the server reports {@code rolledBack=false} - meaning it no
   * longer has the transaction to roll back, typically because the idle reaper already reclaimed it - this
   * method throws a {@link TransactionException} instead of silently reporting a clean rollback. The local
   * transaction state ({@code transactionId}/{@code sessionId}) is still cleared before the throw, so the
   * client is left in a consistent, tx-free state.
   *
   * <p>This is a backward-incompatible change for callers that use {@code rollback()} in a {@code catch}/
   * cleanup block: such callers should wrap the call in {@code try/catch} if a throw from an already-reaped
   * transaction must not mask an earlier exception.
   */
  @Override
  public void rollback() {

    checkDatabaseIsOpen();
    stats.txRollbacks.incrementAndGet();

    if (transactionId == null)
      throw new TransactionException("Transaction not begun");

    logTx("ROLLBACK(local)", null);
    checkCrossThreadUse("before RollbackTransaction");

    try {
      callUnaryVoid("RollbackTransaction", () -> {

      try {

        RollbackTransactionRequest request = RollbackTransactionRequest.newBuilder()
            .setTransaction(TransactionContext.newBuilder().setTransactionId(transactionId).setDatabase(getName()).build())
            .setCredentials(buildCredentials()).build();

        try {

          RollbackTransactionResponse response = blockingStub.withDeadlineAfter(getTimeout(), TimeUnit.MILLISECONDS)
              .rollbackTransaction(request);

          LogManager.instance()
              .log(this, Level.FINE, "[After rollback] Success: %s Committed: %s", response.getSuccess(),
                  response.getRolledBack());

          if (!response.getSuccess()) {
            throw new TransactionException("Failed to rollback transaction: " + response.getMessage());
          }
          // rolledBack=false means the server had no such transaction to roll back (e.g. it was already
          // reaped). Surface it instead of silently reporting a clean rollback.
          if (!response.getRolledBack()) {
            throw new TransactionException("Transaction was not rolled back on the server: " + response.getMessage());
          }
        } catch (StatusRuntimeException | StatusException e) {
          throw new TransactionException("Error on transaction rollback", e);
        } finally {
          transactionId = null;
          setSessionId(null);
        }

        if (debugTx != null) {
          debugTx.rolledBack = true;
          debugTx.rpcSeq.incrementAndGet();
        }
        logTx("ROLLBACK sent", "RollbackTransaction");
      } finally {
        debugTx = null;
      }

      });
    } finally {
      // RESET CREATED RECORDS SO THEY CAN BE CLEANLY RE-INSERTED AFTER THE ROLLBACK (ISSUE #4562)
      resetCreatedRecordsIdentity();
    }
  }

  /**
   * The base {@link RemoteDatabase#transaction(com.arcadedb.database.Database.TransactionScope)}
   * implementation, on an uncaught exception or a retryable failure, only clears the local
   * {@code sessionId} on the client without telling the server to roll back. With the HTTP
   * transport this is benign because the server-side session times out, but with gRPC the
   * server keeps the {@code TransactionContext} (and its dedicated executor thread) registered
   * in {@code activeTransactions} until the client explicitly commits, rolls back or closes
   * the connection. That leaks server-side resources and, once {@code executeQuery} honors the
   * client's transaction context (fix for #4260), it also makes subsequent queries see the
   * still-active in-flight tx. Whenever sessionId is being cleared while transactionId is
   * still set, perform a best-effort rollback so the server cleans up immediately.
   */
  @Override
  protected void setSessionId(final String sessionId) {
    if (sessionId == null && transactionId != null) {
      try {
        rollback();
      } catch (final Throwable ignore) {
        // Best-effort: ensure local state is cleared even if the rollback RPC fails.
        transactionId = null;
        super.setSessionId(null);
      }
      return;
    }
    super.setSessionId(sessionId);
  }

  @Override
  public void deleteRecord(final Record record) {
    checkDatabaseIsOpen();
    stats.deleteRecord.incrementAndGet();

    if (record.getIdentity() == null)
      throw new IllegalArgumentException("Cannot delete a non persistent record");

    final DeleteRecordRequest.Builder deleteBuilder =
        DeleteRecordRequest.newBuilder().setDatabase(getName()).setRid(record.getIdentity().toString())
            .setCredentials(buildCredentials());

    if (transactionId != null)
      deleteBuilder.setTransaction(TransactionContext.newBuilder().setTransactionId(transactionId).setDatabase(getName()).build());

    final DeleteRecordRequest req = deleteBuilder.build();

    try {
      if (LogManager.instance().isDebugEnabled()) {
        LogManager.instance().log(this, Level.FINE, "CLIENT deleteRecord: db=%s, tx=%s, rid=%s", getName(),
            transactionId != null,
            record.getIdentity());
      }

      final DeleteRecordResponse resp = callUnary("DeleteRecord",
          () -> blockingStub.withDeadlineAfter(getTimeout(), TimeUnit.MILLISECONDS).deleteRecord(req));

      // Prefer the proto's 'deleted' flag (your other overload uses it)
      if (!resp.getDeleted()) {
        throw new DatabaseOperationException(
            "Failed to delete record: " + (resp.getMessage().isEmpty() ? "unknown error" : resp.getMessage()));
      }

    } catch (StatusRuntimeException | StatusException e) {
      handleGrpcException(e); // rethrows mapped domain exception
      throw new IllegalStateException("unreachable");
    }
  }

  public boolean deleteRecord(final String rid, final long timeoutMs) {
    checkDatabaseIsOpen();
    stats.deleteRecord.incrementAndGet();

    final DeleteRecordRequest.Builder deleteBuilder = DeleteRecordRequest.newBuilder().setDatabase(getName()).setRid(rid)
        .setCredentials(buildCredentials());

    if (transactionId != null)
      deleteBuilder.setTransaction(TransactionContext.newBuilder().setTransactionId(transactionId).setDatabase(getName()).build());

    final DeleteRecordRequest req = deleteBuilder.build();

    try {
      if (LogManager.instance().isDebugEnabled()) {
        LogManager.instance()
            .log(this, Level.FINE, "CLIENT deleteRecord: db=%s, tx=%s, rid=%s, timeoutMs=%s", getName(),
                transactionId != null,
                rid, timeoutMs);
      }

      final DeleteRecordResponse res = callUnary("DeleteRecord",
          () -> blockingStub.withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS).deleteRecord(req));

      return res.getDeleted();

    } catch (StatusRuntimeException | StatusException e) {
      handleGrpcException(e); // rethrows mapped domain exception
      throw new IllegalStateException("unreachable");
    }
  }

  @Override
  public ResultSet command(final String language, final String command, final ContextConfiguration configuration,
                           final Object... args) {

    checkDatabaseIsOpen();
    stats.commands.incrementAndGet();

    final Map<String, Object> params = mapArgs(args);

    return commandInternal(language, command, params);
  }

  @Override
  public ResultSet command(final String language, final String command, final ContextConfiguration configuration,
                           final Map<String, Object> params) {
    checkDatabaseIsOpen();
    stats.commands.incrementAndGet();

    return commandInternal(language, command, params);
  }

  @Override
  public ResultSet command(final String language, final String command, final Object... args) {
    checkDatabaseIsOpen();
    stats.commands.incrementAndGet();

    final Map<String, Object> params = mapArgs(args);

    return commandInternal(language, command, params);
  }

  @Override
  public ResultSet command(final String language, final String command, final Map<String, Object> params) {
    checkDatabaseIsOpen();
    stats.commands.incrementAndGet();

    return commandInternal(language, command, params);
  }

  private ResultSet commandInternal(final String language, final String command, final Map<String, Object> params) {

    ExecuteCommandRequest.Builder requestBuilder = ExecuteCommandRequest.newBuilder()
        .setDatabase(getName())
        .setCommand(command)
        .setLanguage(language)
        .setReturnRows(true)
        .setCredentials(buildCredentials());

    if (transactionId != null) {
      requestBuilder.setTransaction(TransactionContext.newBuilder().setTransactionId(transactionId).setDatabase(getName()).build());
    }

    if (params != null && !params.isEmpty()) {
      requestBuilder.putAllParameters(convertParamsToGrpcValue(params));
    }

    try {

      if (LogManager.instance().isDebugEnabled())
        LogManager.instance()
            .log(this, Level.FINE, "CLIENT executeCommand: db=%s, tx=%s, cmdLen=%s, params=%s", getName(),
                transactionId != null,
                requestBuilder.getCommand().length(), requestBuilder.getParametersCount());

      final ExecuteCommandResponse response = callUnary("ExecuteCommand",
          () -> blockingStub.withDeadlineAfter(getTimeout(), TimeUnit.MILLISECONDS).executeCommand(requestBuilder.build()));

      if (LogManager.instance().isDebugEnabled())
        LogManager.instance().log(this, Level.FINE, "CLIENT executeCommand: success = %s", response.getSuccess());

      if (!response.getSuccess()) {
        // Unreachable against a correct server: executeCommandInternal now only ever returns success=true
        // (every failure is rethrown server-side and reaches this client as a StatusRuntimeException, caught
        // below and mapped to the exact engine exception type - issue #6192). Kept and logged loudly rather
        // than removed, as a canary: if a future server-side regression ever reintroduces this in-band
        // envelope, it fails visibly here instead of silently resurrecting the untyped fallback #6192 fixed.
        LogManager.instance().log(this, Level.SEVERE,
            "CLIENT executeCommand: server returned success=false in-band, which should be unreachable after #6192: %s",
            response.getMessage());
        throw new DatabaseOperationException("Failed to execute command: " + response.getMessage());
      }

      ResultSet resultSet;

      resultSet = createGrpcResultSet(response);

      return resultSet;
    } catch (StatusRuntimeException | StatusException e) {
      handleGrpcException(e);
      return new InternalResultSet();
    }
  }

  @Override
  public ResultSet query(final String language, final String query, final Object... args) {
    checkDatabaseIsOpen();
    stats.queries.incrementAndGet();

    final Map<String, Object> params = mapArgs(args);

    RemoteGrpcConfig remoteGrpcConfig = new RemoteGrpcConfig(true, ProjectionEncoding.PROJECTION_AS_JSON, 0);

    return query(language, query, remoteGrpcConfig, params);
  }

  @Override
  public ResultSet query(final String language, final String query, final Map<String, Object> params) {

    RemoteGrpcConfig remoteGrpcConfig = new RemoteGrpcConfig(true, ProjectionEncoding.PROJECTION_AS_JSON, 0);

    return query(language, query, remoteGrpcConfig, params);
  }

  public ResultSet query(final String language, final String query, RemoteGrpcConfig remoteGrpcConfig,
                         final Object... args) {

    final Map<String, Object> params = mapArgs(args);

    return query(language, query, remoteGrpcConfig, params);
  }

  public ResultSet query(final String language, final String query, RemoteGrpcConfig remoteGrpcConfig,
                         final Map<String, Object> params) {

    checkDatabaseIsOpen();
    stats.queries.incrementAndGet();

    ExecuteQueryRequest.Builder requestBuilder = ExecuteQueryRequest.newBuilder()
        .setDatabase(getName())
        .setQuery(query)
        .setLanguage(langOrDefault(language))
        .setCredentials(buildCredentials());

    if (transactionId != null) {
      requestBuilder.setTransaction(TransactionContext.newBuilder().setTransactionId(transactionId).setDatabase(getName()).build());
    }

    if (params != null && !params.isEmpty()) {
      requestBuilder.putAllParameters(convertParamsToGrpcValue(params));
    }

    ProjectionSettings projectionSettings = ProjectionSettings.newBuilder()
        .setIncludeProjections(remoteGrpcConfig.includeProjections())
        .setProjectionEncoding(remoteGrpcConfig.projectionEncoding())
        .setSoftLimitBytes(Int32Value.newBuilder().setValue(remoteGrpcConfig.softLimitBytes()).build()).build();

    requestBuilder.setProjectionSettings(projectionSettings);

    try {

      if (LogManager.instance().isDebugEnabled()) {
        LogManager.instance()
            .log(this, Level.FINE, "CLIENT executeQuery: db=%s, tx=%s, queryLen=%s, params=%s", getName(),
                transactionId != null,
                requestBuilder.getQuery().length(), requestBuilder.getParametersCount());
      }

      final ExecuteQueryRequest req = requestBuilder.build();

      final ExecuteQueryResponse response = callUnary("ExecuteQuery",
          () -> blockingStub.withDeadlineAfter(getTimeout(), TimeUnit.MILLISECONDS) // or getTimeout(MODE_QUERY)
              .executeQuery(req));

      if (LogManager.instance().isDebugEnabled()) {
        int _r = 0;
        for (var qr : response.getResultsList())
          _r += qr.getRecordsCount();
        LogManager.instance().log(this, Level.FINE, "CLIENT executeQuery: results=%s", _r);
      }
      return createGrpcResultSet(response);
    } catch (StatusException | StatusRuntimeException e) {
      handleGrpcException(e);
      return new InternalResultSet();
    }
  }

  public ExecuteCommandResponse execSql(String db, String sql, Map<String, Object> params, long timeoutMs) {
    return executeCommand(db, "sql", sql, params, /* returnRows */ false, /* maxRows */ 0, txBeginCommit(), timeoutMs);
  }

  public ExecuteCommandResponse execSql(String sql, Map<String, Object> params, long timeoutMs) {
    return executeCommand(databaseName, "sql", sql, params, /* returnRows */ false, /* maxRows */ 0, txBeginCommit(),
        timeoutMs);
  }

  public ExecuteCommandResponse executeCommand(String language, String command, Map<String, Object> params,
                                               boolean returnRows,
                                               int maxRows,
                                               TransactionContext tx, long timeoutMs) {

    var reqB = ExecuteCommandRequest.newBuilder().setDatabase(databaseName).setCommand(command)
        .putAllParameters(convertParamsToGrpcValue(params)).setLanguage(langOrDefault(language)).setReturnRows(returnRows)
        .setMaxRows(Math.max(maxRows, 0));

    if (tx != null)
      reqB.setTransaction(tx);
    reqB.setCredentials(buildCredentials());

    try {
      return callUnary("ExecuteCommand",
          () -> blockingStub.withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS).executeCommand(reqB.build()));
    } catch (StatusException | StatusRuntimeException e) {
      // handleGrpcException already called in callUnary, this is unreachable
      throw new IllegalStateException("unreachable");
    }
  }

  public ExecuteCommandResponse executeCommand(String database, String language, String command,
                                               Map<String, Object> params,
                                               boolean returnRows, int maxRows, TransactionContext tx, long timeoutMs) {

    var reqB = ExecuteCommandRequest.newBuilder().setDatabase(database).setCommand(command)
        .putAllParameters(convertParamsToGrpcValue(params)).setLanguage(langOrDefault(language)).setReturnRows(returnRows)
        .setMaxRows(Math.max(maxRows, 0));

    if (tx != null)
      reqB.setTransaction(tx);
    reqB.setCredentials(buildCredentials());

    try {
      return callUnary("ExecuteCommand",
          () -> blockingStub.withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS).executeCommand(reqB.build()));
    } catch (StatusException | StatusRuntimeException e) {
      // handleGrpcException already called in callUnary, this is unreachable
      throw new IllegalStateException("unreachable");
    }
  }

  @Override
  protected RID saveRecord(final MutableDocument record) {
    stats.createRecord.incrementAndGet();

    final RID rid = record.getIdentity();

    if (rid != null) {
      // -------- UPDATE (full replacement) --------
      // save() persists the complete current state of the record, so it must use the full-replacement payload
      // (oneof "record"). A partial payload cannot express a removed property (the key is simply absent from the
      // map), which would leave dropped properties behind on the server. This mirrors the HTTP "update content".
      GrpcRecord fullRecord =
          GrpcRecord.newBuilder().putAllProperties(convertParamsToGrpcValue(record.toMap(false)))
              .build();

      UpdateRecordRequest.Builder updateBuilder = UpdateRecordRequest.newBuilder().setDatabase(getName())
          .setRid(rid.toString()).setRecord(fullRecord).setDatabase(databaseName).setCredentials(buildCredentials());

      if (transactionId != null)
        updateBuilder.setTransaction(TransactionContext.newBuilder().setTransactionId(transactionId).setDatabase(getName()).build());

      try {

        @SuppressWarnings("unused")
        UpdateRecordResponse response =
            blockingStub.withDeadlineAfter(getTimeout(), TimeUnit.MILLISECONDS).updateRecord(updateBuilder.build());

        // If your proto has flags, you can check response.getSuccess()/getUpdated()
        // Otherwise, treat non-exception as success.
        return rid;
      } catch (StatusRuntimeException e) {
        handleGrpcException(e);
        return null;
      } catch (StatusException e) {
        handleGrpcException(e);
        return null;
      }
    } else {
      // -------- CREATE --------
      GrpcRecord recMsg =
          GrpcRecord.newBuilder().putAllProperties(convertParamsToGrpcValue(record.toMap(false))).build();

      CreateRecordRequest.Builder createBuilder =
          CreateRecordRequest.newBuilder().setDatabase(getName()).setType(record.getTypeName())
              .setRecord(recMsg).setCredentials(buildCredentials());

      if (transactionId != null)
        createBuilder.setTransaction(TransactionContext.newBuilder().setTransactionId(transactionId).setDatabase(getName()).build());

      final CreateRecordRequest request = createBuilder.build();

      try {
        CreateRecordResponse response =
            blockingStub.withDeadlineAfter(getTimeout(), TimeUnit.MILLISECONDS).createRecord(request);

        // Proto returns the newly created RID as a string
        final String ridStr = response.getRid();
        if (ridStr == null || ridStr.isEmpty()) {
          throw new DatabaseOperationException("Failed to create record (empty RID)");
        }

        // Construct a DatabaseRID bound to this remote database so asX() resolves correctly.
        final RID newRID = newRID(ridStr);
        trackCreatedRecord(record);
        return newRID;
      } catch (StatusRuntimeException | StatusException e) {
        handleGrpcException(e);
        return null;
      }
    }
  }

  private RemoteGrpcConfig getDefaultRemoteGrpcConfig() {

    return new RemoteGrpcConfig(true, ProjectionEncoding.PROJECTION_AS_JSON, 0);
  }

  // Convenience: default batch size stays 100, default mode = CURSOR

  public ResultSet queryStream(final String language, final String query) {
    return queryStream(language, query, getDefaultRemoteGrpcConfig(), /* batchSize */100,
        StreamQueryRequest.RetrievalMode.CURSOR);
  }

  public ResultSet queryStream(final String language, final String query, final RemoteGrpcConfig config) {
    return queryStream(language, query, config, /* batchSize */100, StreamQueryRequest.RetrievalMode.CURSOR);
  }

  public ResultSet queryStream(final String language, final String query, final int batchSize) {
    return queryStream(language, query, getDefaultRemoteGrpcConfig(), batchSize,
        StreamQueryRequest.RetrievalMode.CURSOR);
  }

  public ResultSet queryStream(final String language, final String query, RemoteGrpcConfig config,
                               final int batchSize) {
    return queryStream(language, query, config, batchSize, StreamQueryRequest.RetrievalMode.CURSOR);
  }

  public ResultSet queryStream(final String language, final String query, final int batchSize,
                               final StreamQueryRequest.RetrievalMode mode) {
    return queryStream(language, query, getDefaultRemoteGrpcConfig(), batchSize, mode);
  }

  // NEW: choose retrieval mode

  public ResultSet queryStream(final String language, final String query, final RemoteGrpcConfig config,
                               final int batchSize,
                               final StreamQueryRequest.RetrievalMode mode) {

    return queryStream(language, query, config, Map.of(), batchSize, mode);
  }

  /**
   * Stream query results as a ResultSet that fetches data lazily in batches. This
   * provides consistency with non-streaming query methods and supports non-Record
   * results like projections and aggregations.
   */
  /**
   * Binds a stream request to the client's currently active transaction, if any, so the server routes the
   * stream to that transaction's dedicated thread and it sees the transaction's own uncommitted writes.
   * Without this a streamed read inside a transaction would run as a throwaway read and miss in-flight
   * changes (issue #5042).
   */
  private void bindActiveTransaction(final StreamQueryRequest.Builder b) {
    if (transactionId != null)
      b.setTransaction(TransactionContext.newBuilder().setTransactionId(transactionId).setDatabase(getName()).build());
  }

  public ResultSet queryStream(final String language, final String query, final RemoteGrpcConfig config,
                               final Map<String, Object> params,
                               final int batchSize, final StreamQueryRequest.RetrievalMode mode) {

    checkDatabaseIsOpen();
    stats.queries.incrementAndGet();

    StreamQueryRequest.Builder b = StreamQueryRequest.newBuilder().setDatabase(getName()).setQuery(query)
        .setLanguage(langOrDefault(language))
        .setCredentials(buildCredentials())
        .setBatchSize(batchSize > 0 ? batchSize : 100).setRetrievalMode(mode);

    if (params != null && !params.isEmpty()) {
      b.putAllParameters(convertParamsToGrpcValue(params));
    }

    bindActiveTransaction(b);

    final BlockingClientCall<?, QueryResult> responseIterator = callServerStreaming("StreamQuery",
        () -> blockingStub.withDeadlineAfter(getTimeout(), TimeUnit.MILLISECONDS).streamQuery(b.build()));

    // Return a streaming ResultSet implementation
    return new StreamingResultSet(responseIterator, this);
  }

  // Keep the old signature working (defaults to CURSOR)
  public ResultSet queryStream(final String language, final String query, final Map<String, Object> params,
                               final int batchSize) {

    return queryStream(language, query, getDefaultRemoteGrpcConfig(), params, batchSize,
        StreamQueryRequest.RetrievalMode.CURSOR);
  }

  public ResultSet queryStream(final String language, final String query, final RemoteGrpcConfig config,
                               final Map<String, Object> params,
                               final int batchSize) {

    return queryStream(language, query, config, params, batchSize, StreamQueryRequest.RetrievalMode.CURSOR);
  }

  /**
   * Enhanced streaming with batch-aware ResultSet for better memory control and
   * performance monitoring.
   */
  public ResultSet queryStreamBatched(final String language, final String query, final Map<String, Object> params,
                                      final int batchSize,
                                      final StreamQueryRequest.RetrievalMode mode) {

    checkDatabaseIsOpen();
    stats.queries.incrementAndGet();

    final StreamQueryRequest.Builder b = StreamQueryRequest.newBuilder().setDatabase(getName()).setQuery(query)
        .setLanguage(langOrDefault(language))
        .setCredentials(buildCredentials()).setBatchSize(batchSize > 0 ? batchSize : 100).setRetrievalMode(mode);

    if (params != null && !params.isEmpty()) {
      b.putAllParameters(convertParamsToGrpcValue(params));
    }

    bindActiveTransaction(b);

    final BlockingClientCall<?, QueryResult> responseIterator = callServerStreaming("StreamQuery",
        () -> blockingStub.withWaitForReady().withDeadlineAfter(getTimeout(), TimeUnit.MILLISECONDS).streamQuery(b.build()));

    return new BatchedStreamingResultSet(responseIterator, this);
  }

  public Iterator<QueryBatch> queryStreamBatchesIterator(final String language, final String query,
                                                         final Map<String, Object> params,
                                                         final int batchSize,
                                                         final StreamQueryRequest.RetrievalMode mode) {

    checkDatabaseIsOpen();
    stats.queries.incrementAndGet();

    final StreamQueryRequest.Builder b = StreamQueryRequest.newBuilder().setDatabase(getName()).setQuery(query)
        .setLanguage(langOrDefault(language))
        .setCredentials(buildCredentials()).setBatchSize(batchSize > 0 ? batchSize : 100).setRetrievalMode(mode);

    if (params != null && !params.isEmpty()) {
      b.putAllParameters(convertParamsToGrpcValue(params));
    }

    bindActiveTransaction(b);

    final BlockingClientCall<?, QueryResult> responseIterator = callServerStreaming("StreamQuery",
        () -> blockingStub.withWaitForReady().withDeadlineAfter(getTimeout(), TimeUnit.MILLISECONDS).streamQuery(b.build()));

    return new Iterator<QueryBatch>() {
      private QueryBatch nextBatch = null;
      private boolean    drained   = false;

      @Override
      public boolean hasNext() {
        if (nextBatch != null)
          return true;
        if (drained)
          return false;

        if (debugTx != null) {
          checkCrossThreadUse("streamQueryBatches.hasNext");
        }

        try {

          while (responseIterator.hasNext()) {
            final QueryResult qr = responseIterator.read();

            int totalInBatch = qr.getTotalRecordsInBatch();
            if (totalInBatch == 0) {
              totalInBatch = qr.getRecordsCount();
            }

            if (qr.getRecordsCount() == 0 && !qr.getIsLastBatch()) {
              continue; // empty non-terminal batch
            }

            // Convert GrpcRecords to Results (not Records)
            final List<Result> convertedResults = new ArrayList<>(qr.getRecordsCount());
            for (GrpcRecord gr : qr.getRecordsList()) {
              Result result = grpcRecordToResult(gr);
              convertedResults.add(result);
            }

            // Create QueryBatch with Results instead of Records
            nextBatch = new QueryBatch(convertedResults, totalInBatch, qr.getRunningTotalEmitted(),
                qr.getIsLastBatch());

            if (qr.getIsLastBatch()) {
              drained = true;
            }
            return true;
          }

          drained = true;
          return false;
        } catch (StatusRuntimeException | StatusException e) {
          handleGrpcException(e);
          throw new IllegalStateException("unreachable");
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw new RuntimeException("Stream interrupted", ie);
        } catch (RuntimeException re) {
          throw re;
        } catch (Exception e) {
          throw new RuntimeException("Stream failed", e);
        }
      }

      @Override
      public QueryBatch next() {
        if (!hasNext())
          throw new NoSuchElementException();
        final QueryBatch out = nextBatch;
        nextBatch = null;
        return out;
      }
    };
  }

  public Iterator<GrpcRecord> queryStream(final String database, final String sql, final Map<String, Object> params,
                                          final int batchSize,
                                          final StreamQueryRequest.RetrievalMode mode, final TransactionContext tx,
                                          final long timeoutMs) {
    final StreamQueryRequest.Builder reqB = StreamQueryRequest.newBuilder().setDatabase(database).setQuery(sql)
        .putAllParameters(convertParamsToGrpcValue(params)).setCredentials(buildCredentials())
        .setBatchSize(batchSize > 0 ? batchSize : 100).setRetrievalMode(mode);

    if (tx != null)
      reqB.setTransaction(tx);

    final BlockingClientCall<?, QueryResult> it = callServerStreaming("StreamQuery",
        () -> blockingStub.withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS).streamQuery(reqB.build()));

    return new Iterator<GrpcRecord>() {
      private Iterator<GrpcRecord> curr = Collections.emptyIterator();

      @Override
      public boolean hasNext() {
        if (curr.hasNext())
          return true;

        if (debugTx != null) {
          checkCrossThreadUse("streamQuery.hasNext");
        }

        try {
          while (it.hasNext()) {
            final QueryResult qr = it.read();
            curr = qr.getRecordsList().iterator();
            if (curr.hasNext())
              return true;
            // else loop to fetch next batch (handles empty batches)
          }
          return false;
        } catch (StatusRuntimeException | StatusException e) {
          handleGrpcException(e);
          throw new IllegalStateException("unreachable");
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw new RuntimeException("Stream interrupted", ie);
        } catch (RuntimeException re) {
          throw re;
        } catch (Exception e) {
          throw new RuntimeException("Stream failed", e);
        }
      }

      @Override
      public GrpcRecord next() {
        if (!hasNext())
          throw new NoSuchElementException();
        if (debugTx != null) {
          checkCrossThreadUse("streamQuery.next");
        }
        return curr.next();
      }
    };
  }

  @Deprecated
  public Iterator<Record> queryStreamAsRecordIterator(final String language, final String query, final Map<String,
                                                          Object> params,
                                                      final int batchSize) {
    ResultSet rs = queryStream(language, query, params, batchSize);

    // Convert ResultSet to Iterator<Record>
    return new Iterator<Record>() {
      @Override
      public boolean hasNext() {
        return rs.hasNext();
      }

      @Override
      public Record next() {
        Result result = rs.next();
        // Try to get Record from Result
        if (result.isElement()) {
          return result.getRecord().orElseThrow(() -> new IllegalStateException("""
              Result claims to be element but has \
              no Record"""));
        }
        // For non-Record results, throw or skip
        throw new IllegalStateException("Result is not a Record: " + result);
      }
    };
  }

  @Override
  public Iterator<Record> iterateType(final String typeName, final boolean polymorphic) {
    String query = "select from `" + typeName + "`";
    if (!polymorphic)
      query += " where @type = '" + typeName + "'";
    return streamQuery(query);
  }

  @Override
  public Iterator<Record> iterateBucket(final String bucketName) {
    return streamQuery("select from bucket:`" + bucketName + "`");
  }

  public String createRecord(final String cls, final Map<String, Object> props, final long timeoutMs) {
    checkDatabaseIsOpen();

    if (cls == null || cls.isBlank())
      throw new IllegalArgumentException("cls must be non-empty");
    if (props == null)
      throw new IllegalArgumentException("props must be non-null");

    // Build payload
    final GrpcRecord recMsg = GrpcRecord.newBuilder().putAllProperties(convertParamsToGrpcValue(props)).build();

    final CreateRecordRequest req =
        CreateRecordRequest.newBuilder().setDatabase(getName()).setType(cls).setRecord(recMsg)
            .setCredentials(buildCredentials()).build();

    try {
      if (LogManager.instance().isDebugEnabled()) {
        LogManager.instance()
            .log(this, Level.FINE, "CLIENT createRecord: db=%s, txOpen=%s, type=%s, propCount=%s, timeoutMs=%s",
                getName(),
                transactionId != null,
                cls, props.size(), timeoutMs);
      }

      final CreateRecordResponse res = callUnary("CreateRecord",
          () -> blockingStub.withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS).createRecord(req));

      return res.getRid(); // e.g. "#12:0"

    } catch (StatusRuntimeException | StatusException e) {
      handleGrpcException(e); // rethrows mapped domain exception
      throw new IllegalStateException("unreachable");
    }
  }

  public String createRecordTx(final String cls, final Map<String, Object> props, final long timeoutMs) {
    checkDatabaseIsOpen();

    final GrpcRecord recMsg = GrpcRecord.newBuilder().putAllProperties(convertParamsToGrpcValue(props)).build();

    final CreateRecordRequest req =
        CreateRecordRequest.newBuilder().setDatabase(getName()).setType(cls).setRecord(recMsg)
            .setTransaction(TransactionContext.newBuilder().setBegin(true).setCommit(true)).setCredentials(buildCredentials()).build();

    try {
      final CreateRecordResponse res = callUnary("CreateRecord",
          () -> blockingStub.withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS).createRecord(req));
      return res.getRid();

    } catch (StatusRuntimeException | StatusException e) {
      handleGrpcException(e);
      throw new IllegalStateException("unreachable");
    }
  }

  public boolean updateRecord(String rid, Map<String, Object> props, long timeoutMs) {
    PropertiesUpdate partial = PropertiesUpdate.newBuilder().putAllProperties(convertParamsToGrpcValue(props)).build();

    return updateRecord(rid, partial, timeoutMs);
  }

  public boolean updateRecord(String rid, Record dbRecord, long timeoutMs) {
    GrpcRecord record = ProtoUtils.toProtoRecord(dbRecord);
    return updateRecordFull(rid, record, timeoutMs);
  }

  private boolean updateRecord(final String rid, final PropertiesUpdate partial, final long timeoutMs) {
    checkDatabaseIsOpen();

    if (rid == null || rid.isBlank())
      throw new IllegalArgumentException("rid must be non-empty");
    if (partial == null)
      throw new IllegalArgumentException("partial must be non-null");

    final UpdateRecordRequest req =
        UpdateRecordRequest.newBuilder().setDatabase(getName()).setRid(rid).setPartial(partial)
            // Per-call tx: begin+commit. If you have an outer tx, pass it instead.
            .setTransaction(TransactionContext.newBuilder().setBegin(true).setCommit(true)).setCredentials(buildCredentials()).build();

    try {
      if (LogManager.instance().isDebugEnabled()) {
        LogManager.instance()
            .log(this, Level.FINE, "CLIENT updateRecord(partial): db=%s, txOpen=%s, rid=%s, timeoutMs=%s", getName(),
                transactionId != null,
                rid,
                timeoutMs);
      }

      final UpdateRecordResponse res = callUnary("UpdateRecord",
          () -> blockingStub.withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS).updateRecord(req));

      // Most builds expose getSuccess(); if your proto has getUpdated(), swap here.
      return res.getSuccess();

    } catch (StatusRuntimeException | StatusException e) {
      handleGrpcException(e); // rethrows mapped domain exception
      throw new IllegalStateException("unreachable");
    }
  }

  private boolean updateRecordFull(final String rid, final GrpcRecord record, final long timeoutMs) {
    checkDatabaseIsOpen();

    if (rid == null || rid.isBlank())
      throw new IllegalArgumentException("rid must be non-empty");
    if (record == null)
      throw new IllegalArgumentException("record must be non-null");

    final UpdateRecordRequest req =
        UpdateRecordRequest.newBuilder().setDatabase(getName()).setRid(rid).setRecord(record)
            // Per-call tx: begin+commit. If you have an outer tx, pass it instead.
            .setTransaction(TransactionContext.newBuilder().setBegin(true).setCommit(true)).setCredentials(buildCredentials()).build();

    try {
      if (LogManager.instance().isDebugEnabled()) {
        LogManager.instance().log(this, Level.FINE, """
                CLIENT updateRecord(full): db=%s, txOpen=%s, rid=%s, \
                timeoutMs=%s""", getName(),
            transactionId != null, rid,
            timeoutMs);
      }

      final UpdateRecordResponse res = callUnary("UpdateRecord",
          () -> blockingStub.withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS).updateRecord(req));

      return res.getSuccess();

    } catch (StatusRuntimeException | StatusException e) {
      handleGrpcException(e); // rethrows mapped domain exception
      throw new IllegalStateException("unreachable");
    }
  }

  @Override
  public RemoteTransactionExplicitLock acquireLock() {
    // Need gRPC-specific implementation
    if (explicitLock == null)
      explicitLock = new RemoteGrpcTransactionExplicitLock(this);

    return explicitLock;
  }

  @Override
  public long countBucket(final String bucketName) {
    checkDatabaseIsOpen();
    stats.countBucket.incrementAndGet();
    ResultSet result = query("sql", "select count(*) as count from bucket:" + bucketName);
    if (result.hasNext()) {
      Number count = result.next().getProperty("count");
      return count != null ? count.longValue() : 0;
    }
    return 0;
  }

  @Override
  public long countType(final String typeName, final boolean polymorphic) {
    checkDatabaseIsOpen();
    stats.countType.incrementAndGet();
    final String appendix = polymorphic ? "" : " where @type = '" + typeName + "'";
    ResultSet result = query("sql", "select count(*) as count from " + typeName + appendix);
    if (result.hasNext()) {
      Number count = result.next().getProperty("count");
      return count != null ? count.longValue() : 0;
    }
    return 0;
  }

  @Override
  public Record lookupByRID(final RID rid) {
    return lookupByRID(rid, true);
  }

  @Override
  public Record lookupByRID(final RID rid, final boolean loadContent) {
    checkDatabaseIsOpen();
    stats.readRecord.incrementAndGet();

    if (rid == null)
      throw new IllegalArgumentException("Record is null");

    final LookupByRidRequest.Builder reqBuilder = LookupByRidRequest.newBuilder().setDatabase(getName()).setRid(rid.toString())
        .setCredentials(buildCredentials());

    // Propagate the active transaction so the read is tracked by it (required for REPEATABLE_READ conflict detection).
    if (transactionId != null)
      reqBuilder.setTransaction(TransactionContext.newBuilder().setTransactionId(transactionId).setDatabase(getName()).build());

    final LookupByRidRequest req = reqBuilder.build();

    try {
      if (LogManager.instance().isDebugEnabled()) {
        LogManager.instance()
            .log(this, Level.FINE, "CLIENT lookupByRID: db=%s, txOpen=%s, rid=%s, loadContent=%s, timeoutMs=%s",
                getName(),
                transactionId != null,
                rid, loadContent, getTimeout());
      }

      final LookupByRidResponse resp = callUnary("LookupByRid",
          () -> blockingStub.withDeadlineAfter(getTimeout(), TimeUnit.MILLISECONDS).lookupByRid(req));

      if (!resp.getFound())
        throw new RecordNotFoundException("Record " + rid + " not found", rid);

      // Note: loadContent is currently a no-op for gRPC; response already carries the
      // record.
      final Record record = grpcRecordToDBRecord(resp.getRecord());
      if (record == null)
        throw new RecordNotFoundException("Record " + rid + " not found", rid);

      return record;

    } catch (StatusException | StatusRuntimeException e) {
      handleGrpcException(e); // maps & rethrows proper domain exception
      throw new IllegalStateException("unreachable");
    }
  }

  @Override
  public boolean existsRecord(RID rid) {
    stats.existsRecord.incrementAndGet();
    if (rid == null)
      throw new IllegalArgumentException("Record is null");
    try {
      lookupByRID(rid, false);
      return true;
    } catch (RecordNotFoundException e) {
      return false;
    }
  }

  // Convenience overload that accepts domain rows (convert first)
  public InsertSummary insertBulkAsListOfMaps(final InsertOptions options, final List<Map<String, Object>> rows,
                                              final long timeoutMs) {

    List<GrpcRecord> protoRows = rows.stream().map(this::toProtoRecordFromMap) // your converter
        .collect(Collectors.toList());

    return insertBulk(options, protoRows, timeoutMs);
  }

  public InsertSummary insertBulk(final InsertOptions options, final List<GrpcRecord> protoRows, final long timeoutMs) {

    // Ensure options carry DB + credentials as the server expects
    final InsertOptions.Builder ob = options.toBuilder();

    final String dbInOpts = options.getDatabase();
    if (dbInOpts == null || dbInOpts.isEmpty()) {
      ob.setDatabase(getName());
    }

    final boolean missingCreds = !options.hasCredentials() || options.getCredentials().getUsername().isEmpty();
    if (missingCreds) {
      ob.setCredentials(buildCredentials());
    }

    final InsertOptions newOptions = ob.build();

    final BulkInsertRequest req = BulkInsertRequest.newBuilder().setOptions(newOptions).addAllRows(protoRows).build();

    try {
      if (LogManager.instance().isDebugEnabled()) {
        LogManager.instance()
            .log(this, Level.FINE, "CLIENT insertBulk: rows=%s, timeoutMs=%s, tx=%s", req.getRowsCount(), timeoutMs,
                transactionId != null);
      }

      // use callUnary so tx cross-thread checks + rpcSeq happen in one place
      return callUnary("BulkInsert",
          () -> blockingStub.withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS).bulkInsert(req));

    } catch (StatusRuntimeException | StatusException e) {
      handleGrpcException(e); // maps to your domain exceptions and rethrows
      throw new IllegalStateException("unreachable"); // keep compiler happy
    }
  }

  /**
   * Returns a builder for a batch graph import that travels over the {@code GraphBatchLoad} streaming RPC, the
   * gRPC transport this connection was opened for, rather than as JSONL over HTTP the way the inherited
   * implementation would (issue #6070). The builder and the loader it produces expose the same API as the HTTP
   * ones, so this override is transparent to a caller that only uses {@code batch()}.
   *
   * @return a new {@link RemoteGrpcGraphBatch.Builder}
   */
  @Override
  public RemoteGrpcGraphBatch.Builder batch() {
    checkDatabaseIsOpen();
    return new RemoteGrpcGraphBatch.Builder(this);
  }

  /**
   * Opens one {@code GraphBatchLoad} call and returns its client end. Package-visible so
   * {@link RemoteGrpcGraphBatch} can drive the stream without reaching for the stub itself.
   */
  GraphBatchLoadStream openGraphBatchLoadStream(final long timeoutMs) {
    final GraphBatchLoadStream stream = new GraphBatchLoadStream(timeoutMs);
    stream.start(callAsyncDuplex("GraphBatchLoad", timeoutMs,
        (stub, responseObserver) -> stub.graphBatchLoad(responseObserver),
        stream.responseObserver()));
    return stream;
  }

  // Convenience overload
  public InsertSummary ingestStreamAsListOfMaps(final InsertOptions options, final List<Map<String, Object>> rows,
                                                final int chunkSize,
                                                final long timeoutMs) throws InterruptedException {

    List<GrpcRecord> protoRows = rows.stream().map(this::toProtoRecordFromMap).collect(Collectors.toList());

    return ingestStream(options, protoRows, chunkSize, timeoutMs);
  }

  public InsertSummary ingestStream(final InsertOptions options, final List<GrpcRecord> protoRows, final int chunkSize,
                                    final long timeoutMs)
      throws InterruptedException {

    checkDatabaseIsOpen();

    if (protoRows == null || protoRows.isEmpty())
      return InsertSummary.newBuilder().setReceived(0).build();

    if (chunkSize <= 0)
      throw new IllegalArgumentException("chunkSize must be > 0");

    // Ensure options carry DB + credentials
    final InsertOptions.Builder ob = options.toBuilder();
    if (options.getDatabase() == null || options.getDatabase().isEmpty())
      ob.setDatabase(getName());
    if (!options.hasCredentials() || options.getCredentials().getUsername().isEmpty())
      ob.setCredentials(buildCredentials());
    final InsertOptions effOptions = ob.build();

    final CountDownLatch done = new CountDownLatch(1);
    final AtomicReference<InsertSummary> summaryRef = new AtomicReference<>();
    final AtomicReference<Throwable> errorRef = new AtomicReference<>();

    final StreamObserver<InsertSummary> resp = new StreamObserver<>() {
      @Override
      public void onNext(InsertSummary value) {
        summaryRef.set(value);
      }

      @Override
      public void onError(Throwable t) {
        errorRef.set(t);
        done.countDown();
      }

      @Override
      public void onCompleted() {
        done.countDown();
      }
    };

    if (LogManager.instance().isDebugEnabled()) {
      LogManager.instance()
          .log(this, Level.FINE, "CLIENT ingestStream: db=%s, rows=%s, chunkSize=%s, timeoutMs=%s", getName(),
              protoRows.size(),
              chunkSize,
              timeoutMs);
    }

    // Open the client stream via wrapper (adds deadline, tx checks, unified error
    // mapping)
    final StreamObserver<InsertChunk> req = callAsyncDuplex("InsertStream", timeoutMs,
        (stub, responseObserver) -> stub.insertStream(responseObserver), wrapObserver("InsertStream", resp));

    final String sessionId = "sess-" + System.nanoTime();
    long seq = 1;
    int sent = 0;

    try {
      for (int i = 0; i < protoRows.size(); i += chunkSize) {
        final int end = Math.min(i + chunkSize, protoRows.size());

        // (Sending options on every chunk is safe; if you prefer first-chunk-only, gate
        // on seq == 1.)
        final InsertChunk chunk =
            InsertChunk.newBuilder().setSessionId(sessionId).setOptions(effOptions).setChunkSeq(seq++)
                .addAllRows(protoRows.subList(i, end)).build();

        req.onNext(chunk);
        sent += end - i;
      }
      req.onCompleted();
    } catch (RuntimeException sendErr) {
      // Client-side failure during onNext/onCompleted: propagate
      throw sendErr;
    }

    // Wait for server final summary or error (tiny grace after deadline)
    final boolean finished = done.await(Math.max(1, timeoutMs) + 1_000, TimeUnit.MILLISECONDS);
    if (!finished) {
      throw new TimeoutException("ingestStream timed out waiting for server completion");
    }

    // If wrapObserver mapped an error, rethrow it directly
    final Throwable streamErr = errorRef.get();
    if (streamErr != null) {
      if (streamErr instanceof RuntimeException re)
        throw re;
      throw new RemoteException("gRPC stream failed: " + streamErr.getMessage(), streamErr);
    }

    InsertSummary s = summaryRef.get();
    if (s == null) {
      // Fallback if server completed without emitting a final summary
      s = InsertSummary.newBuilder().setReceived(sent).build();
    }

    if (LogManager.instance().isDebugEnabled()) {
      LogManager.instance().log(this, Level.FINE, "CLIENT ingestStream: completed; received=%s", s.getReceived());
    }

    return s;
  }

  /**
   * Pushes domain {@code com.arcadedb.database.Record} rows via
   * InsertBidirectional with per-batch ACKs.
   */
  public InsertSummary ingestBidi(final List<Record> rows, final InsertOptions opts, final int chunkSize,
                                  final int maxInflight, final long timeoutMs) throws InterruptedException {

    return ingestBidiCore(rows, opts, chunkSize, maxInflight, timeoutMs,
        (Object o) -> toProtoRecordFromDbRecord((Record) o));
  }

  public InsertSummary ingestBidi(final List<Record> rows, final InsertOptions opts, final int chunkSize,
                                  final int maxInflight) throws InterruptedException {

    return ingestBidiCore(rows, opts, chunkSize, maxInflight, /* timeoutMs */ 5 * 60_000L,
        this::toProtoRecordFromDbRecord);
  }

  /**
   * Pushes map-shaped rows (property map per row) via InsertBidirectional with
   * per-batch ACKs.
   */
  public InsertSummary ingestBidi(final InsertOptions options, final List<Map<String, Object>> rows,
                                  final int chunkSize,
                                  final int maxInflight) throws InterruptedException {

    return ingestBidiCore(rows, options, chunkSize, maxInflight, /* timeoutMs */ 5 * 60_000L,
        this::toProtoRecordFromMap);
  }

  public InsertSummary ingestBidi(final InsertOptions options, final List<Map<String, Object>> rows,
                                  final int chunkSize,
                                  final int maxInflight, final long timeoutMs) throws InterruptedException {

    return ingestBidiCore(rows, options, chunkSize, maxInflight, timeoutMs, this::toProtoRecordFromMap);
  }

  /**
   * Core implementation of InsertBidirectional ingest
   */

  private <T> InsertSummary ingestBidiCore(final List<T> rows, final InsertOptions options, final int chunkSize,
                                           final int maxInflight,
                                           final long timeoutMs, final Function<? super T, GrpcRecord> mapper) throws InterruptedException {

    // Fast-path & guards
    if (rows == null || rows.isEmpty())
      return InsertSummary.newBuilder().setReceived(0).build();
    if (chunkSize <= 0)
      throw new IllegalArgumentException("chunkSize must be > 0");
    if (maxInflight <= 0)
      throw new IllegalArgumentException("maxInflight must be > 0");

    // Options with DB + creds
    final InsertOptions.Builder ob = options.toBuilder();
    if (options.getDatabase() == null || options.getDatabase().isEmpty())
      ob.setDatabase(getName());
    if (!options.hasCredentials() || options.getCredentials().getUsername().isEmpty())
      ob.setCredentials(buildCredentials());
    final InsertOptions effectiveOpts = ob.build();

    // Pre-map rows → proto
    final List<GrpcRecord> protoRows = rows.stream().map(mapper).collect(Collectors.toList());

    if (LogManager.instance().isDebugEnabled()) {
      LogManager.instance()
          .log(this, Level.FINE, "CLIENT ingestBidi start: rows=%s, chunkSize=%s, maxInflight=%s, timeoutMs=%s",
              protoRows.size(),
              chunkSize,
              maxInflight, timeoutMs);
    }

    // --- streaming state
    final String sessionId = "sess-" + System.nanoTime();
    final CountDownLatch done = new CountDownLatch(1);
    final AtomicReference<Throwable> errRef = new AtomicReference<>();
    final AtomicLong seq = new AtomicLong(1);
    final AtomicInteger cursor = new AtomicInteger(0);
    final AtomicInteger sent = new AtomicInteger(0);
    final AtomicInteger acked = new AtomicInteger(0);
    final AtomicReference<InsertSummary> committed = new AtomicReference<>();
    final List<BatchAck> acks = Collections.synchronizedList(new ArrayList<>());

    final AtomicReference<ClientCallStreamObserver<InsertRequest>> observerRef = new AtomicReference<>();

    final AtomicBoolean commitSent = new AtomicBoolean(false);
    final Object streamLock = new Object();
    final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(
        r -> {
          Thread t = new Thread(r, "grpc-ack-grace-timer");
          t.setDaemon(true);
          return t;
        });
    final long ackGraceMillis = Math.min(Math.max(timeoutMs / 10, 1_000L), 10_000L);
    final Object timerLock = new Object();
    final AtomicReference<ScheduledFuture<?>> ackGraceFuture = new AtomicReference<>();

    final Runnable sendCommitIfNeeded = () -> {
      synchronized (streamLock) {
        if (commitSent.compareAndSet(false, true)) {
          try {
            final ClientCallStreamObserver<InsertRequest> r = observerRef.get();
            if (r != null) {
              r.onNext(InsertRequest.newBuilder().setCommit(Commit.newBuilder().setSessionId(sessionId)).build());
              r.onCompleted();
            }
          } catch (Throwable t) {
            // Best effort - stream may be closed
            if (LogManager.instance().isDebugEnabled())
              LogManager.instance().log(this, Level.FINE, "CLIENT ingestBidi commit failed (best effort): %s",
                  t.getMessage());
          }
        }
      }
    };

    final Runnable armAckGraceTimer = () -> {
      synchronized (timerLock) {
        var prev = ackGraceFuture.getAndSet(null);
        if (prev != null)
          prev.cancel(false);
        if (cursor.get() >= protoRows.size() && acked.get() < sent.get() && !commitSent.get()) {
          var fut = scheduler.schedule(sendCommitIfNeeded, ackGraceMillis, TimeUnit.MILLISECONDS);
          ackGraceFuture.set(fut);
        }
      }
    };

    final Runnable cancelAckGraceTimer = () -> {
      synchronized (timerLock) {
        var prev = ackGraceFuture.getAndSet(null);
        if (prev != null)
          prev.cancel(false);
      }
    };

    // --- response observer (keeps backpressure via beforeStart)
    final ClientResponseObserver<InsertRequest, InsertResponse> respObserver = new ClientResponseObserver<>() {
      ClientCallStreamObserver<InsertRequest> req;
      volatile boolean started = false;

      @Override
      public void beforeStart(ClientCallStreamObserver<InsertRequest> r) {
        this.req = r;
        observerRef.set(r);
        r.disableAutoInboundFlowControl();
        r.setOnReadyHandler(this::drain);
      }

      private void drain() {
        synchronized (streamLock) {
          if (commitSent.get())
            return;
          if (!req.isReady())
            return;

          if (!started) {
            req.onNext(InsertRequest.newBuilder().setStart(Start.newBuilder().setOptions(effectiveOpts)).build());
            started = true;
            req.request(1); // pull first server response
          }

          while (req.isReady() && (sent.get() - acked.get()) < maxInflight) {
            final int start = cursor.get();
            if (start >= protoRows.size())
              break;

            final int end = Math.min(start + chunkSize, protoRows.size());
            final var slice = protoRows.subList(start, end);

            final var chunk =
                InsertChunk.newBuilder().setSessionId(sessionId).setChunkSeq(seq.getAndIncrement()).addAllRows(slice)
                    .build();

            req.onNext(InsertRequest.newBuilder().setChunk(chunk).build());
            cursor.set(end);
            sent.incrementAndGet();
          }

          if (cursor.get() >= protoRows.size()) {
            if (acked.get() >= sent.get())
              sendCommitIfNeeded.run();
            else
              armAckGraceTimer.run();
          }
        }
      }

      @Override
      public void onNext(InsertResponse v) {
        switch (v.getMsgCase()) {
          case STARTED -> {
            drain();
          }
          case BATCH_ACK -> {
            acks.add(v.getBatchAck());
            acked.incrementAndGet();
            // free capacity & manage timers
            drain();
            if (cursor.get() >= protoRows.size()) {
              if (acked.get() >= sent.get()) {
                cancelAckGraceTimer.run();
                sendCommitIfNeeded.run();
              } else {
                armAckGraceTimer.run();
              }
            }
          }
          case COMMITTED -> {
            committed.set(v.getCommitted().getSummary());
            cancelAckGraceTimer.run();
          }
          case ERROR -> {
            // surface as error; caller will map/throw after await
            errRef.set(new StatusRuntimeException(Status.INTERNAL.withDescription(v.getError().getMessage())));
            cancelAckGraceTimer.run();
            try {
              req.cancel("server ERROR", null);
            } catch (Throwable ignore) {
            }
          }
          case MSG_NOT_SET -> {
            /* ignore */
          }
        }
        req.request(1); // keep pulling
      }

      @Override
      public void onError(Throwable t) {
        cancelAckGraceTimer.run();
        errRef.set(t);
        done.countDown();
      }

      @Override
      public void onCompleted() {
        cancelAckGraceTimer.run();
        done.countDown();
      }
    };

    // --- open bidi via wrapper (deadline, tx checks, unified logging)
    @SuppressWarnings("unused") final StreamObserver<InsertRequest> _req = callAsyncDuplex("InsertBidirectional",
        timeoutMs,
        (stub, responseObs) -> stub.insertBidirectional(responseObs),
        wrapClientResponseObserver("InsertBidirectional", respObserver) // preserves
        // beforeStart
    );

    try {
      // wait for completion (tiny grace after deadline)
      final boolean finished = done.await(Math.max(1, timeoutMs) + 1_000, TimeUnit.MILLISECONDS);
      if (!finished) {
        final ClientCallStreamObserver<InsertRequest> r = observerRef.get();
        if (r != null) {
          try {
            r.cancel("client timeout waiting for completion", null);
          } catch (Throwable ignore) {
          }
        }
        throw new TimeoutException("ingestBidirectional timed out waiting for server completion");
      }

      final Throwable err = errRef.get();
      if (err != null) {
        // wrapClientResponseObserver already runs through handleGrpcException();
        // if it was a non-gRPC Throwable, surface it uniformly here.
        if (err instanceof RuntimeException re)
          throw re;
        throw new RemoteException("gRPC bidi stream failed: " + err.getMessage(), err);
      }

      if (LogManager.instance().isDebugEnabled()) {
        try {
          LogManager.instance().log(this, Level.FINE, "CLIENT ingestBidi finished: sent=%s, acked=%s", sent.get(),
              acked.get());
        } catch (Throwable ignore) {
        }
      }
    } finally {
      scheduler.shutdownNow();
    }

    // Prefer COMMITTED summary; else aggregate ACKs
    final InsertSummary finalSummary = committed.get();
    if (finalSummary != null)
      return finalSummary;

    final long ins = acks.stream().mapToLong(BatchAck::getInserted).sum();
    final long upd = acks.stream().mapToLong(BatchAck::getUpdated).sum();
    final long ign = acks.stream().mapToLong(BatchAck::getIgnored).sum();
    final long fail = acks.stream().mapToLong(BatchAck::getFailed).sum();

    return InsertSummary.newBuilder().setReceived(protoRows.size()).setInserted(ins).setUpdated(upd).setIgnored(ign).setFailed(fail)
        .build();
  }

  // Map -> GrpcRecord
  private GrpcRecord toProtoRecordFromMap(Map<String, Object> row) {
    GrpcRecord.Builder b = GrpcRecord.newBuilder();
    row.forEach((k, v) -> b.putProperties(k, objectToGrpcValue(v)));
    GrpcRecord rec = b.build();
    if (LogManager.instance().isDebugEnabled())
      LogManager.instance().log(this, Level.FINE, "CLIENT toProtoRecordFromMap: %s", summarize(rec));
    return rec;
  }

  // Domain Record (storage) -> GrpcRecord
  private GrpcRecord toProtoRecordFromDbRecord(Record rec) {
    // Use ProtoUtils for proper conversion
    GrpcRecord out = ProtoUtils.toProtoRecord(rec);
    if (LogManager.instance().isDebugEnabled())
      LogManager.instance().log(this, Level.FINE, "CLIENT toProtoRecordFromDbRecord: %s", summarize(out));
    return out;
  }

  // Convert Java object -> GrpcValue (extend as needed)
  private GrpcValue objectToGrpcValue(Object v) {
    return ProtoUtils.toGrpcValue(v);
  }

  public DatabaseCredentials buildCredentials() {
    return DatabaseCredentials.newBuilder().setUsername(getUserName()).setPassword(getUserPassword()).build();
  }

  private TransactionIsolation mapIsolationLevel(Database.TRANSACTION_ISOLATION_LEVEL level) {
    switch (level) {
//      case READ_UNCOMMITTED:
//        return TransactionIsolation.READ_UNCOMMITTED;
      case READ_COMMITTED:
        return TransactionIsolation.READ_COMMITTED;
      case REPEATABLE_READ:
        return TransactionIsolation.REPEATABLE_READ;
//      case SERIALIZABLE:
//        return TransactionIsolation.SERIALIZABLE;
      default:
        return TransactionIsolation.READ_COMMITTED;
    }
  }

  // ---- TX helpers -------------------------------------------------------------
  private static TransactionContext txBeginCommit() {
    return TransactionContext.newBuilder().setBegin(true).setCommit(true).build();
  }

  private static TransactionContext txNone() {
    return TransactionContext.getDefaultInstance();
  }

  // Optional: language defaulting (server defaults to "sql" too)
  private static String langOrDefault(String language) {
    return language == null || language.isEmpty() ? "sql" : language;
  }

  private Iterator<Record> streamQuery(final String query) {
    StreamQueryRequest request = StreamQueryRequest.newBuilder().setDatabase(getName()).setQuery(query)
        .setLanguage("sql")
        .setCredentials(buildCredentials())
        .setBatchSize(100).build();

    final BlockingClientCall<?, QueryResult> responseIterator = callServerStreaming("StreamQuery",
        () -> blockingStub.withDeadlineAfter(getTimeout(), TimeUnit.MILLISECONDS).streamQuery(request));

    return new Iterator<Record>() {
      private Iterator<GrpcRecord> currentBatch = Collections.emptyIterator();

      @Override
      public boolean hasNext() {

        if (currentBatch.hasNext()) {
          return true;
        }

        try {
          if (responseIterator.hasNext()) {

            QueryResult result;
            result = responseIterator.read();
            currentBatch = result.getRecordsList().iterator();
            return currentBatch.hasNext();
          }
        } catch (InterruptedException | StatusException e) {
          throw new RuntimeException(e);
        }

        return false;
      }

      @Override
      public Record next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        return grpcRecordToDBRecord(currentBatch.next());
      }
    };
  }

  private ResultSet createGrpcResultSet(ExecuteQueryResponse response) {
    InternalResultSet resultSet = new InternalResultSet();
    for (QueryResult queryResult : response.getResultsList()) {
      for (GrpcRecord record : queryResult.getRecordsList()) {
        resultSet.add(grpcRecordToResult(record));
      }
    }
    return resultSet;
  }

  private ResultSet createGrpcResultSet(ExecuteCommandResponse response) {
    InternalResultSet resultSet = new InternalResultSet();
    for (GrpcRecord record : response.getRecordsList()) {
      resultSet.add(grpcRecordToResult(record));
    }
    return resultSet;
  }

  Result grpcRecordToResult(GrpcRecord grpcRecord) {

    Record record = grpcRecordToDBRecord(grpcRecord);

    if (record == null) {

      Map<String, Object> properties = new HashMap<>();
      grpcRecord.getPropertiesMap().forEach((k, v) -> properties.put(k, grpcValueToObject(v)));
      return new ResultInternal(properties);
    }

    return new ResultInternal(record);
  }

  private Record grpcRecordToDBRecord(final GrpcRecord grpcRecord) {
    final Map<String, Object> map = new HashMap<>();

    // Convert properties
    grpcRecord.getPropertiesMap().forEach((k, v) -> map.put(k, grpcValueToObject(v)));

    // Proto3 empty == absent: non-addressable rows (e.g. time-series points) have no @rid/@type.
    if (!grpcRecord.getRid().isEmpty())
      map.put("@rid", grpcRecord.getRid());
    if (!grpcRecord.getType().isEmpty())
      map.put("@type", grpcRecord.getType());

    // The wire's @cat is trustworthy for the CATEGORY (issue #6404), but constructing a concrete
    // RemoteImmutableDocument/Vertex/Edge still needs the type resolvable client-side - its constructor looks
    // the type up eagerly and throws SchemaException if it can't. Unlike @cat, the client's schema cache has no
    // way to know about a type created after it was last (or never) loaded, and RemoteSchema only reloads a
    // NEVER-loaded cache, not a stale one: a type created after the first schema access on this connection stays
    // invisible until something explicitly invalidates the cache. Before @cat was sent on the wire this same
    // existsType() check was where the category came from in the first place (mapRecordType(), below), so an
    // unresolvable type was already silently downgraded to the property-only fallback instead of a crash -
    // trusting the wire for the category must not remove that safety net. Checked once, up front, so the
    // @cat-absent fallback below can reuse the confirmed type instead of resolving it a second time.
    final String typeName = grpcRecord.getType();
    if (typeName.isEmpty() || !getSchema().existsType(typeName))
      return null;

    final GrpcValue catFromGrpcRecord = grpcRecord.getPropertiesMap().get("@cat");
    final String cat = catFromGrpcRecord != null ? catFromGrpcRecord.getStringValue() : mapRecordType(typeName);

    if (cat == null)
      return null;

    map.put("@cat", cat);

    return switch (cat) {
      case "d" -> new RemoteImmutableDocument(this, map);
      case "v" -> new RemoteImmutableVertex(this, map);
      case "e" -> new RemoteImmutableEdge(this, map);
      default -> null;
    };
  }

  /** {@code typeName} must already be confirmed to exist client-side - see the {@code existsType()} check above. */
  private String mapRecordType(final String typeName) {
    try {
      final Object type = getSchema().getType(typeName);
      return switch (type) {
        case VertexType v -> "v";
        case EdgeType e -> "e";
        case DocumentType d -> "d";
        default -> null;
      };

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private Map<String, GrpcValue> convertParamsToGrpcValue(final Map<String, Object> params) {
    final Map<String, GrpcValue> grpcParams = new HashMap<>();

    for (Map.Entry<String, Object> entry : params.entrySet()) {
      GrpcValue value = objectToGrpcValue(entry.getValue());
      grpcParams.put(entry.getKey(), value);
    }

    return grpcParams;
  }

  private Object grpcValueToObject(final GrpcValue grpcValue) {
    Object out = ProtoUtils.fromGrpcValue(grpcValue);
    if (LogManager.instance().isDebugEnabled())
      LogManager.instance()
          .log(this, Level.FINE, "CLIENT decode grpcValueToObject: %s -> %s", summarize(grpcValue), summarize(out));
    return out;
  }

  void handleGrpcException(Throwable e) {
    // Reconstruct the exact engine exception type from the server's status + trailers so type-driven
    // behavior (e.g. RemoteDatabase.transaction() retry on NeedRetryException) works over gRPC exactly as
    // over HTTP. Falls back to status-code-only mapping when the server did not ship the class-name trailer.
    throw GrpcClientErrorMapper.toException(e);
  }

  // --- Debug helpers (client) ---
  private static String summarize(final Object o) {
    try {
      switch (o) {
        case null:
          return "null";
        case CharSequence s:
          return "String(" + s.length() + ")";
        case byte[] b:
          return "bytes[" + b.length + "]";
        case Collection<?> c:
          return o.getClass().getSimpleName() + "[size=" + c.size() + "]";
        case Map<?, ?> m:
          return o.getClass().getSimpleName() + "[size=" + m.size() + "]";
        default:
          return o.getClass().getSimpleName();
      }
    } catch (Exception e) {
      return o.getClass().getSimpleName();
    }
  }

  private static String summarize(final GrpcValue v) {
    if (v == null)
      return "GrpcValue(null)";
    return switch (v.getKindCase()) {
      case BOOL_VALUE -> "BOOL";
      case INT32_VALUE -> "INT32";
      case INT64_VALUE -> "INT64";
      case FLOAT_VALUE -> "FLOAT";
      case DOUBLE_VALUE -> "DOUBLE";
      case STRING_VALUE -> "STRING(" + v.getStringValue().length() + ")";
      case BYTES_VALUE -> "BYTES[" + v.getBytesValue().size() + "]";
      case TIMESTAMP_VALUE -> "TIMESTAMP";
      case LIST_VALUE -> "LIST[" + v.getListValue().getValuesCount() + "]";
      case MAP_VALUE -> "MAP[" + v.getMapValue().getEntriesCount() + "]";
      case EMBEDDED_VALUE -> "EMBEDDED";
      case LINK_VALUE -> "LINK(" + v.getLinkValue().getRid() + ")";
      case DECIMAL_VALUE -> "DECIMAL(scale=" + v.getDecimalValue().getScale() + ")";
      case KIND_NOT_SET -> "KIND_NOT_SET";
    };
  }

  private static String summarize(GrpcRecord r) {
    if (r == null)
      return "GrpcRecord(null)";
    String rid = r.getRid();
    String ty = r.getType();
    int props = r.getPropertiesCount();
    return "GrpcRecord{rid=" + rid + ", type=" + ty + ", props=" + props + "}";
  }

  // RemoteGrpcDatabase.java

  // one place to handle JDK 17/21 differences
  private static String tidName(Thread t) {
    try {
      // Java 19+: threadId()
      long tid = (long) Thread.class.getMethod("threadId").invoke(t);
      return tid + ":" + t.getName();
    } catch (ReflectiveOperationException ignore) {
      return legacyTidName(t);
    }
  }

  private static String legacyTidName(Thread t) {
    return t.threadId() + ":" + t.getName();
  }

  // Optional knobs you can toggle from the TM for a single run
  private volatile boolean txDebugEnabled = true;

  public void enableTxDebug(boolean on) {
    this.txDebugEnabled = on;
  }

  public boolean isLocalTxActive() {
    return debugTx != null;
  }

  public @Nullable String currentLocalTxId() {
    return debugTx != null ? Long.toString(debugTx.id) : null;
  }

  public void setCurrentTxLabel(String label) {
    if (debugTx != null)
      debugTx.txLabel = label;
  }

  private void logTx(String phase, String rpcOp) {
    if (debugTx == null || !LogManager.instance().isDebugEnabled())
      return;
    TxDebug d = debugTx;
    LogManager.instance().log(this, Level.FINE,
        "TXDBG %s db=%s tx#%s label=%s owner=%s now=%s rpcOp=%s rpcSeq=%s beginSent=%s committed=%s rolledBack=%s",
        phase,
        d.dbName,
        d.id, d.txLabel, tidName(d.ownerThread), tidName(Thread.currentThread()), rpcOp, d.rpcSeq.get(), d.beginRpcSent,
        d.committed,
        d.rolledBack);
  }

  void checkCrossThreadUse(String where) {
    TxDebug d = debugTx;
    if (d == null)
      return;
    Thread now = Thread.currentThread();
    if (now != d.ownerThread) {
      LogManager.instance()
          .log(this, Level.WARNING, "TXDBG CROSS-THREAD %s db=%s tx#%s owner=%s now=%s label=%s (begin site follows)"
              , where,
              d.dbName, d.id,
              tidName(d.ownerThread), tidName(now), d.txLabel, d.beginSite);
    }
  }

  // ═══════════════════════════════════════════════════════════════════
  //  Vector, hybrid and full-text retrieval (issue #7306)
  // ═══════════════════════════════════════════════════════════════════

  /**
   * kNN search over a dense {@code LSM_VECTOR} or sparse {@code LSM_SPARSE_VECTOR} index.
   * <p>
   * The request and response shapes, and every bound the server enforces on them, are the ones the HTTP
   * {@code POST /api/v1/vector/{database}/search} route and the MCP {@code vector_search} tool share: the server
   * runs one implementation behind all three. ArcadeDB does not generate embeddings - the caller supplies
   * {@code queryVector}.
   * <p>
   * An argument outside its bound comes back as INVALID_ARGUMENT carrying the same message the HTTP surface
   * answers 400 with, so a client can distinguish "I asked for something illegal" from "the server broke".
   */
  public VectorSearchResponse vectorSearch(final VectorSearchRequest request) {
    final VectorSearchRequest.Builder builder = request.toBuilder()
        .setDatabase(getName()).setCredentials(buildCredentials());
    if (transactionId != null)
      builder.setTransaction(openTransaction());

    return searchCall("VectorSearch", builder.build(),
        req -> blockingStub.withDeadlineAfter(getTimeout(), TimeUnit.MILLISECONDS).vectorSearch(req));
  }

  /**
   * Fused vector + full-text + graph-expansion search. Same sharing and same error contract as
   * {@link #vectorSearch(VectorSearchRequest)}.
   */
  public HybridSearchResponse hybridSearch(final HybridSearchRequest request) {
    final HybridSearchRequest.Builder builder = request.toBuilder()
        .setDatabase(getName()).setCredentials(buildCredentials());
    if (transactionId != null)
      builder.setTransaction(openTransaction());

    return searchCall("HybridSearch", builder.build(),
        req -> blockingStub.withDeadlineAfter(getTimeout(), TimeUnit.MILLISECONDS).hybridSearch(req));
  }

  /**
   * Full-text search over a {@code FULL_TEXT} index. Same sharing and same error contract as
   * {@link #vectorSearch(VectorSearchRequest)}.
   */
  public FullTextSearchResponse fullTextSearch(final FullTextSearchRequest request) {
    final FullTextSearchRequest.Builder builder = request.toBuilder()
        .setDatabase(getName()).setCredentials(buildCredentials());
    if (transactionId != null)
      builder.setTransaction(openTransaction());

    return searchCall("FullTextSearch", builder.build(),
        req -> blockingStub.withDeadlineAfter(getTimeout(), TimeUnit.MILLISECONDS).fullTextSearch(req));
  }

  /**
   * The {@code TransactionContext} naming this connection's open transaction, in the shape every other
   * transaction-scoped RPC on this class already sends. Callers guard on {@code transactionId != null} first;
   * this only assembles the message.
   */
  private TransactionContext openTransaction() {
    return TransactionContext.newBuilder().setTransactionId(transactionId).setDatabase(getName()).build();
  }

  /**
   * Runs one of the three search RPCs. The database name, the credentials and this connection's open
   * transaction are stamped on by the caller rather than left to the application, so a request built by hand
   * cannot address a database other than the one this connection is open on, and a search issued inside a
   * transaction reads inside it (issue #7326) the way the HTTP routes always have through the
   * {@code arcadedb-session-id} header.
   */
  private <Req, Resp> Resp searchCall(final String operation, final Req request, final SearchRpc<Req, Resp> rpc) {
    checkDatabaseIsOpen();
    stats.queries.incrementAndGet();
    try {
      return callUnary(operation, () -> rpc.run(request));
    } catch (final StatusRuntimeException | StatusException e) {
      handleGrpcException(e); // rethrows the mapped domain exception
      throw new IllegalStateException("unreachable");
    }
  }

  @FunctionalInterface
  private interface SearchRpc<Req, Resp> {
    Resp run(Req request) throws StatusException;
  }

  @FunctionalInterface
  private interface Rpc<T> {
    T run() throws StatusException; // V2 throws this; v1 lambdas compile fine too
  }

  // ---------------------------------------------------------------------------------------------------------
  // Time series API over gRPC (issue #7305)
  //
  // These override the HTTP implementations inherited from RemoteDatabase, so the same application code runs
  // over either protocol and the two are directly testable against each other. Only the transport differs: the
  // server side of both reaches the samples through the one shared TimeSeriesGateway.
  // ---------------------------------------------------------------------------------------------------------

  @Override
  public TimeSeriesWriteSummary timeSeriesWrite(final List<TimeSeriesPoint> points) {
    checkDatabaseIsOpen();
    if (points == null || points.isEmpty())
      return TimeSeriesWriteSummary.empty();

    final TimeSeriesWriteRequest.Builder request = TimeSeriesWriteRequest.newBuilder()
        .setDatabase(getName())
        .setCredentials(buildCredentials())
        // The engine stores milliseconds and TimeSeriesPoint carries milliseconds, so the RPC's zero-value
        // precision is already the right one; set explicitly so the intent survives a future default change.
        .setPrecision(TimeSeriesPrecision.TS_PRECISION_MILLISECONDS);

    for (final TimeSeriesPoint point : points)
      request.addPoints(toGrpcPoint(point));

    try {
      final com.arcadedb.server.grpc.TimeSeriesWriteSummary response = callUnary("TimeSeriesWrite",
          () -> blockingStub.withDeadlineAfter(getTimeout(), TimeUnit.MILLISECONDS)
              .timeSeriesWrite(request.build()));
      return toWriteSummary(response);
    } catch (final StatusRuntimeException | StatusException e) {
      handleGrpcException(e);
      throw new IllegalStateException("unreachable");
    }
  }

  /**
   * Ingests through the client-streaming {@code TimeSeriesWriteStream} RPC: one call, one chunk per
   * {@code chunkSize} points, one summary. This is the shape time-series ingest is best served by, and the
   * reason the RPC exists - the HTTP implementation this overrides can only issue one request per chunk.
   * <p>
   * Still not atomic: each chunk's measurements commit their own shard transactions as they are appended, so a
   * failure part-way leaves the earlier chunks durable.
   * <p>
   * <b>The whole stream runs under one deadline</b> - {@link #getTimeout()}, which defaults to
   * {@code arcadedb.network.socketTimeout} (30 s) - because a gRPC deadline covers the call, not each message.
   * An ingest that takes longer than that fails with DEADLINE_EXCEEDED however many chunks it had already made
   * durable, so a caller feeding a large batch through here should raise {@code setTimeout} first, or split the
   * batch across several calls.
   */
  @Override
  public TimeSeriesWriteSummary timeSeriesWriteStream(final Iterable<TimeSeriesPoint> points, final int chunkSize) {
    checkDatabaseIsOpen();
    if (chunkSize <= 0)
      throw new IllegalArgumentException("chunkSize must be positive");

    final CountDownLatch completed = new CountDownLatch(1);
    final AtomicReference<com.arcadedb.server.grpc.TimeSeriesWriteSummary> summary = new AtomicReference<>();
    final AtomicReference<Throwable> failure = new AtomicReference<>();

    final StreamObserver<TimeSeriesWriteChunk> requests = callAsyncDuplex("TimeSeriesWriteStream", getTimeout(),
        (stub, responseObserver) -> stub.timeSeriesWriteStream(responseObserver),
        new StreamObserver<com.arcadedb.server.grpc.TimeSeriesWriteSummary>() {
          @Override
          public void onNext(final com.arcadedb.server.grpc.TimeSeriesWriteSummary value) {
            summary.set(value);
          }

          @Override
          public void onError(final Throwable t) {
            failure.set(t);
            completed.countDown();
          }

          @Override
          public void onCompleted() {
            completed.countDown();
          }
        });

    int sent = 0;
    try {
      TimeSeriesWriteChunk.Builder chunk = newWriteChunk(true);
      for (final TimeSeriesPoint point : points) {
        chunk.addPoints(toGrpcPoint(point));
        if (chunk.getPointsCount() >= chunkSize) {
          requests.onNext(chunk.build());
          sent++;
          chunk = newWriteChunk(false);
        }
      }
      // The first chunk carries the database, so an empty stream still has to send it: without a chunk the
      // server never resolves a database and answers an empty summary, which is the right answer for no
      // points anyway - so only send a trailing chunk when it holds something, or when nothing was sent yet
      // and the caller therefore expects the (empty) round trip.
      if (chunk.getPointsCount() > 0 || sent == 0)
        requests.onNext(chunk.build());
      requests.onCompleted();
    } catch (final RuntimeException e) {
      requests.onError(e);
      throw e;
    }

    try {
      if (!completed.await(getTimeout(), TimeUnit.MILLISECONDS))
        throw new RemoteException("Timeout waiting for the TimeSeriesWriteStream summary");
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RemoteException("Interrupted while waiting for the TimeSeriesWriteStream summary", e);
    }

    if (failure.get() != null) {
      // callAsyncDuplex wraps the response observer, and that wrapper has ALREADY run the failure through
      // handleGrpcException before handing it here - so this is the engine exception type the server named
      // (SecurityException for a denied type, DuplicatedKeyException, ...), not a raw gRPC status. Mapping it a
      // second time finds no status on it and flattens every one of them to "gRPC error: UNKNOWN", which is how
      // a PERMISSION_DENIED on the streaming write stopped being distinguishable from any other failure.
      final Throwable cause = failure.get();
      if (cause instanceof RuntimeException mapped)
        throw mapped;
      handleGrpcException(cause);
    }

    final com.arcadedb.server.grpc.TimeSeriesWriteSummary response = summary.get();
    if (response == null)
      throw new RemoteException("TimeSeriesWriteStream completed without a summary");

    return toWriteSummary(response);
  }

  @Override
  public TimeSeriesQueryResult timeSeriesQuery(final TimeSeriesQuery query) {
    checkDatabaseIsOpen();
    stats.queries.incrementAndGet();

    final TimeSeriesQueryRequest.Builder request = TimeSeriesQueryRequest.newBuilder()
        .setDatabase(getName())
        .setCredentials(buildCredentials())
        .setType(query.getType());

    if (query.getFromTimestamp() != null)
      request.setFromTimestamp(query.getFromTimestamp());
    if (query.getToTimestamp() != null)
      request.setToTimestamp(query.getToTimestamp());
    request.addAllFields(query.getFields());
    if (!query.getTags().isEmpty())
      request.setTags(toGrpcTagFilter(query.getTags()));
    if (query.getLimit() > 0)
      request.setLimit(query.getLimit());

    if (query.isAggregated()) {
      final TimeSeriesAggregation.Builder aggregation = TimeSeriesAggregation.newBuilder()
          .setBucketIntervalMs(query.getBucketIntervalMs());
      for (final TimeSeriesQuery.Aggregation requested : query.getAggregations())
        aggregation.addRequests(TimeSeriesAggregationRequest.newBuilder()
            .setField(requested.field())
            .setType(toGrpcAggregationType(requested.type()))
            .setAlias(requested.resolvedAlias())
            .build());
      request.setAggregation(aggregation.build());
    }

    final BlockingClientCall<?, com.arcadedb.server.grpc.TimeSeriesQueryResult> stream =
        callServerStreaming("TimeSeriesQuery",
            () -> blockingStub.withDeadlineAfter(getTimeout(), TimeUnit.MILLISECONDS)
                .timeSeriesQuery(request.build()));

    final List<String> columns = new ArrayList<>();
    final List<String> aggregations = new ArrayList<>();
    final List<Object[]> rows = new ArrayList<>();
    final List<TimeSeriesBucket> buckets = new ArrayList<>();
    boolean truncated = false;

    try {
      while (stream.hasNext()) {
        final com.arcadedb.server.grpc.TimeSeriesQueryResult message = stream.read();
        if (message == null)
          break;
        // The header repeats on every message; take it from the first one that carries it so a caller reading
        // an answer with no rows at all still learns the column names.
        if (columns.isEmpty())
          columns.addAll(message.getColumnsList());
        if (aggregations.isEmpty())
          aggregations.addAll(message.getAggregationsList());
        for (final TimeSeriesRow row : message.getRowsList())
          rows.add(fromGrpcRow(row));
        for (final com.arcadedb.server.grpc.TimeSeriesBucket bucket : message.getBucketsList())
          buckets.add(new TimeSeriesBucket(bucket.getTimestamp(), fromGrpcValues(bucket.getValuesList())));
        if (message.getLast())
          truncated = message.getTruncated();
      }
    } catch (final StatusRuntimeException | StatusException e) {
      // BlockingClientCall reports a failed stream as a checked StatusException on hasNext()/read(); map it to
      // the engine exception type the server named, exactly as a unary call's StatusRuntimeException is.
      handleGrpcException(e);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RemoteException("Interrupted while reading the TimeSeriesQuery stream", e);
    }

    // An aggregated answer carries no columns and a raw one carries no aggregations, which is what
    // TimeSeriesQueryResult.isAggregated() reads; keep the two apart rather than filling both.
    return new TimeSeriesQueryResult(query.getType(), aggregations.isEmpty() ? columns : List.of(),
        rows, aggregations, buckets, truncated);
  }

  @Override
  public TimeSeriesLatestResult timeSeriesLatest(final String typeName, final String tagName,
      final Object tagValue) {
    checkDatabaseIsOpen();

    final TimeSeriesLatestRequest.Builder request = TimeSeriesLatestRequest.newBuilder()
        .setDatabase(getName())
        .setCredentials(buildCredentials())
        .setType(typeName);
    if (tagName != null && !tagName.isBlank())
      request.setTags(toGrpcTagFilter(Map.of(tagName, tagValue)));

    try {
      final TimeSeriesLatestResponse response = callUnary("TimeSeriesLatest",
          () -> blockingStub.withDeadlineAfter(getTimeout(), TimeUnit.MILLISECONDS)
              .timeSeriesLatest(request.build()));

      return new TimeSeriesLatestResult(response.getType(), response.getColumnsList(),
          response.getFound() ? fromGrpcRow(response.getLatest()) : null);
    } catch (final StatusRuntimeException | StatusException e) {
      handleGrpcException(e);
      throw new IllegalStateException("unreachable");
    }
  }

  /** The first chunk of a write stream carries the database and credentials; later ones do not need to. */
  private TimeSeriesWriteChunk.Builder newWriteChunk(final boolean first) {
    final TimeSeriesWriteChunk.Builder chunk = TimeSeriesWriteChunk.newBuilder()
        .setPrecision(TimeSeriesPrecision.TS_PRECISION_MILLISECONDS);
    if (first)
      chunk.setDatabase(getName()).setCredentials(buildCredentials());
    return chunk;
  }

  private static com.arcadedb.server.grpc.TimeSeriesPoint toGrpcPoint(final TimeSeriesPoint point) {
    final com.arcadedb.server.grpc.TimeSeriesPoint.Builder builder =
        com.arcadedb.server.grpc.TimeSeriesPoint.newBuilder()
            .setType(point.type())
            .setTimestamp(point.timestampMs());
    for (final Map.Entry<String, Object> tag : point.tags().entrySet())
      if (tag.getValue() != null)
        builder.putTags(tag.getKey(), ProtoUtils.toGrpcValue(tag.getValue()));
    for (final Map.Entry<String, Object> field : point.fields().entrySet())
      if (field.getValue() != null)
        // A null field is how a sample says "no measurement for this column"; sending it as an unset GrpcValue
        // would be indistinguishable from a value the server should store, so it is simply omitted - which is
        // exactly what the line-protocol writer does on the HTTP path.
        builder.putFields(field.getKey(), ProtoUtils.toGrpcValue(field.getValue()));
    return builder.build();
  }

  private static TimeSeriesTagFilter toGrpcTagFilter(final Map<String, Object> tags) {
    final TimeSeriesTagFilter.Builder filter = TimeSeriesTagFilter.newBuilder();
    for (final Map.Entry<String, Object> tag : tags.entrySet())
      filter.putEquals(tag.getKey(), ProtoUtils.toGrpcValue(tag.getValue()));
    return filter.build();
  }

  private static TimeSeriesAggregationType toGrpcAggregationType(final AggregationType type) {
    return switch (type) {
      case SUM -> TimeSeriesAggregationType.TS_AGG_SUM;
      case AVG -> TimeSeriesAggregationType.TS_AGG_AVG;
      case MIN -> TimeSeriesAggregationType.TS_AGG_MIN;
      case MAX -> TimeSeriesAggregationType.TS_AGG_MAX;
      case COUNT -> TimeSeriesAggregationType.TS_AGG_COUNT;
    };
  }

  private static Object[] fromGrpcRow(final TimeSeriesRow row) {
    return fromGrpcValues(row.getValuesList());
  }

  private static Object[] fromGrpcValues(final List<GrpcValue> values) {
    final Object[] decoded = new Object[values.size()];
    for (int i = 0; i < decoded.length; i++)
      // An unset GrpcValue decodes to null, which is how both protocols spell "no measurement".
      decoded[i] = ProtoUtils.fromGrpcValue(values.get(i));
    return decoded;
  }

  private static TimeSeriesWriteSummary toWriteSummary(
      final com.arcadedb.server.grpc.TimeSeriesWriteSummary response) {
    return new TimeSeriesWriteSummary(response.getReceived(), response.getWritten(), response.getDropped(),
        response.getUnknownTypesList(), response.getNonTimeSeriesTypesList(), response.getUnavailableTypesList());
  }

  private <Resp> Resp callUnary(String opName, Rpc<Resp> rpc) throws StatusException {
    if (debugTx != null) {
      checkCrossThreadUse("RPC " + opName);
      logTx("RPC(local)", opName);
    }
    try {
      return rpc.run();
    } finally {
      if (debugTx != null) {
        debugTx.rpcSeq.incrementAndGet();
      }
    }
  }

  // unary that returns void
  private void callUnaryVoid(String opName, Runnable rpc) {
    if (debugTx != null) {
      checkCrossThreadUse("RPC " + opName);
      logTx("RPC(unary)", opName);
    }
    rpc.run();
    if (debugTx != null) {
      debugTx.rpcSeq.incrementAndGet();
    }
  }

  // Helper for server-streaming RPCs (mirrors callUnary)
  private <Resp> BlockingClientCall<?, Resp> callServerStreaming(String opName,
                                                                 Supplier<BlockingClientCall<?, Resp>> rpc) {
    if (debugTx != null) {
      checkCrossThreadUse("RPC " + opName);
      logTx("RPC(open-stream)", opName);
    }
    try {
      final BlockingClientCall<?, Resp> it = rpc.get();
      if (debugTx != null) {
        debugTx.rpcSeq.incrementAndGet();
      }
      return it;
    } catch (StatusRuntimeException e) {
      handleGrpcException(e); // rethrows mapped runtime exception
      throw new IllegalStateException("unreachable");
    }
  }

  // For async "client-streaming" and "bidirectional" calls that RETURN a request
  // StreamObserver
  private <Req, Resp> StreamObserver<Req> callAsyncDuplex(String opName, long timeoutMs,
                                                          BiFunction<ArcadeDbServiceGrpc.ArcadeDbServiceStub,
                                                              StreamObserver<Resp>, StreamObserver<Req>> starter,
                                                          StreamObserver<Resp> responseObserver) {
    if (debugTx != null) {
      checkCrossThreadUse("STREAM " + opName);
      logTx("STREAM(local)", opName);
    }
    final var stub = asyncStub.withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS);
    // Don't double-wrap if the observer is already a ClientResponseObserver (e.g. from wrapClientResponseObserver)
    // because wrapping it again with wrapObserver would hide the ClientResponseObserver interface
    // and prevent beforeStart() from being called.
    StreamObserver<Resp> effectiveObserver = responseObserver instanceof ClientResponseObserver
        ? responseObserver
        : wrapObserver(opName, responseObserver);
    StreamObserver<Req> reqObs = starter.apply(stub, effectiveObserver);
    if (debugTx != null) {
      debugTx.rpcSeq.incrementAndGet();
    }
    return reqObs;
  }

  // For async "server-streaming" calls that take (request, responseObserver) and
  // return void
  private <Req, Resp> void callAsyncServerStreaming(String opName, long timeoutMs, Req request,
                                                    BiConsumer<ArcadeDbServiceGrpc.ArcadeDbServiceStub,
                                                        StreamObserver<Resp>> invoker,
                                                    StreamObserver<Resp> responseObserver) {
    if (debugTx != null) {
      checkCrossThreadUse("STREAM " + opName);
      logTx("STREAM(local)", opName);
    }
    final var stub = asyncStub.withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS);
    invoker.accept(stub, wrapObserver(opName, responseObserver));
    if (debugTx != null) {
      debugTx.rpcSeq.incrementAndGet();
    }
  }

  // Wrap a plain StreamObserver to translate gRPC Status into your domain
  // exceptions
  private <T> StreamObserver<T> wrapObserver(String opName, StreamObserver<T> delegate) {
    return new StreamObserver<>() {
      @Override
      public void onNext(T value) {
        delegate.onNext(value);
      }

      @Override
      public void onError(Throwable t) {
        try {
          handleGrpcException(t);
        } catch (RuntimeException mapped) {
          delegate.onError(mapped);
          return;
        }
        delegate.onError(t);
      }

      @Override
      public void onCompleted() {
        delegate.onCompleted();
      }
    };
  }

  // Same idea, but preserves ClientResponseObserver features (beforeStart, flow
  // control)
  private <Req, Resp> ClientResponseObserver<Req, Resp> wrapObserver(String opName,
                                                                     ClientResponseObserver<Req, Resp> delegate) {
    return new ClientResponseObserver<>() {
      @Override
      public void beforeStart(ClientCallStreamObserver<Req> r) {
        // pass through; your delegate may set onReady handler, request(n), etc.
        delegate.beforeStart(r);
      }

      @Override
      public void onNext(Resp value) {
        delegate.onNext(value);
      }

      @Override
      public void onError(Throwable t) {
        try {
          handleGrpcException(t);
        } catch (RuntimeException mapped) {
          delegate.onError(mapped);
          return;
        }
        delegate.onError(t);
      }

      @Override
      public void onCompleted() {
        delegate.onCompleted();
      }
    };
  }

  private <ReqT, RespT> ClientResponseObserver<ReqT, RespT> wrapClientResponseObserver(String opName,
                                                                                       ClientResponseObserver<ReqT,
                                                                                           RespT> delegate) {
    return new ClientResponseObserver<>() {
      @Override
      public void beforeStart(ClientCallStreamObserver<ReqT> requestStream) {
        // Preserve delegate’s backpressure hooks
        delegate.beforeStart(requestStream);
      }

      @Override
      public void onNext(RespT value) {
        delegate.onNext(value);
      }

      @Override
      public void onError(Throwable t) {
        // Normalize gRPC errors through your handler, then pass the mapped exception
        try {
          if (t instanceof StatusRuntimeException sre) {
            handleGrpcException(sre); // throws
          } else if (t instanceof StatusException se) {
            handleGrpcException(se); // throws
          }
          // Non-gRPC error: forward as-is
          delegate.onError(t);
        } catch (RuntimeException mapped) {
          // forward the mapped exception to delegate
          delegate.onError(mapped);
        }
      }

      @Override
      public void onCompleted() {
        delegate.onCompleted();
      }
    };
  }
}
