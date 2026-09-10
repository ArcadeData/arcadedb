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
package com.arcadedb.remote;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.BasicDatabase;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.Database;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.database.async.ErrorCallback;
import com.arcadedb.database.async.OkCallback;
import com.arcadedb.exception.ArcadeDBException;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.DatabaseIsClosedException;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.executor.InternalResultSet;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.BinarySerializer;
import com.arcadedb.engine.timeseries.LineProtocolWriter;
import com.arcadedb.remote.timeseries.TimeSeriesBucket;
import com.arcadedb.remote.timeseries.TimeSeriesLatestResult;
import com.arcadedb.remote.timeseries.TimeSeriesPoint;
import com.arcadedb.remote.timeseries.TimeSeriesQuery;
import com.arcadedb.remote.timeseries.TimeSeriesQueryResult;
import com.arcadedb.remote.timeseries.TimeSeriesWriteSummary;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.Pair;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URLEncoder;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.logging.Level;

import static com.arcadedb.schema.Property.CAT_PROPERTY;
import static com.arcadedb.schema.Property.RID_PROPERTY;

/**
 * Remote Database implementation. It's not thread safe. For multi-thread usage create one instance of RemoteDatabase
 * per thread.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class RemoteDatabase extends RemoteHttpComponent implements BasicDatabase {
  /**
   * Media type of the HTTP streaming query encoding (issue #7306), sent in {@code Accept} to select it. The
   * server keeps answering the buffered {@code application/json} body to anything else, which is what let this
   * be added without changing a single existing response.
   * <p>
   * Deliberately duplicated as {@code NdJsonResultStream.CONTENT_TYPE} in the {@code server} module: this module
   * cannot depend on it. Change one and you must change the other, or this driver stops selecting the encoding
   * and silently falls back to the buffered body.
   */
  public static final String NDJSON_CONTENT_TYPE = "application/x-ndjson";

  public static final String ARCADEDB_SESSION_ID = "arcadedb-session-id";

  private final    String                               databaseName;
  private          BinarySerializer                     serializer;
  private          String                               sessionId;
  private          Database.TRANSACTION_ISOLATION_LEVEL transactionIsolationLevel =
      Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED;
  private final    RemoteSchema                         schema                    = new RemoteSchema(this);
  private          boolean                              open                      = true;
  // Records created inside the current transaction. If the transaction is rolled back (explicitly or by a failed
  // commit), their server-assigned RID no longer exists, so the identity is reset to provisional letting the same
  // in-memory object be cleanly re-inserted in a later transaction, matching the embedded engine (issue #4562).
  protected final  List<MutableDocument>                txCreatedRecords          = new ArrayList<>();
  private          RemoteTransactionExplicitLock        explicitLock;
  private          int                                  cachedHashCode            = 0;
  private volatile ReadConsistency                      readConsistency           = ReadConsistency.EVENTUAL;
  private final    AtomicLong                           lastCommitIndex           = new AtomicLong(-1L);
  private volatile int                                  electionRetryCount;
  private volatile long                                 electionRetryDelayMs;

  public RemoteDatabase(final String server, final int port, final String databaseName, final String userName,
                        final String userPassword) {
    this(server, port, databaseName, userName, userPassword, new ContextConfiguration());
  }

  public RemoteDatabase(final String server, final int port, final String databaseName, final String userName,
                        final String userPassword, final ContextConfiguration configuration) {
    super(server, port, userName, userPassword, configuration);
    this.databaseName = databaseName;
    // Issue #7163: no longer wrapped in a catch that logged and carried on with a NULL serializer. An
    // unresolvable arcadedb.dateImplementation now fails the construction with a ConfigurationException naming
    // the class, which is the better of the two: the degraded path only deferred the failure to the first
    // record this database tried to serialize, as a NullPointerException with nothing to say about the cause.
    this.serializer = new BinarySerializer(configuration);
    this.electionRetryCount = configuration.getValueAsInteger(GlobalConfiguration.HA_CLIENT_ELECTION_RETRY_COUNT);
    this.electionRetryDelayMs = configuration.getValueAsLong(GlobalConfiguration.HA_CLIENT_ELECTION_RETRY_DELAY_MS);
  }

  @Override
  public String getName() {
    return databaseName;
  }

  @Override
  public String getDatabasePath() {
    return protocol + "://" + currentServer + ":" + currentPort + "/" + databaseName;
  }

  @Override
  public long getSize() {
    checkDatabaseIsOpen();
    try (final ResultSet resultSet = command("sql", "select size from schema:database")) {
      final Result result = resultSet.nextIfAvailable();
      if (result != null)
        return (long) Type.convert(null, result.getProperty("size"), Long.class);
      return 0L;
    }
  }

  @Override
  public boolean isOpen() {
    return open;
  }

  @Override
  public RemoteSchema getSchema() {
    return schema;
  }

  @Override
  public void close() {
    super.close();
    setSessionId(null);
    open = false;
  }

  @Override
  public void drop() {
    checkDatabaseIsOpen();
    try {
      final JSONObject jsonRequest = new JSONObject().put("command", "drop database " + databaseName);
      String payload = getRequestPayload(jsonRequest);

      HttpRequest request =
          createRequestBuilder("POST", getUrl("server")).POST(HttpRequest.BodyPublishers.ofString(payload))
              .header("Content-Type", "application/json").build();

      HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

      if (response.statusCode() != 200) {
        final Exception detail = manageException(response, "drop database");
        throw new RemoteException("Error on deleting database", detail);
      }

    } catch (final Exception e) {
      throw new DatabaseOperationException("Error on deleting database", e);
    }
    close();
  }

  @Override
  public MutableDocument newDocument(final String typeName) {
    checkDatabaseIsOpen();
    if (typeName == null)
      throw new IllegalArgumentException("Type is null");

    return new RemoteMutableDocument(this, typeName);
  }

  @Override
  public int hashCode() {
    if (cachedHashCode == 0 && getDatabasePath() != null)
      cachedHashCode = getDatabasePath().hashCode();
    return cachedHashCode;
  }

  @Override
  public RemoteMutableVertex newVertex(final String typeName) {
    checkDatabaseIsOpen();
    if (typeName == null)
      throw new IllegalArgumentException("Type is null");

    return new RemoteMutableVertex(this, typeName);
  }

  @Override
  public void transaction(final TransactionScope txBlock) {
    transaction(txBlock, true, configuration.getValueAsInteger(GlobalConfiguration.TX_RETRIES), null, null);
  }

  @Override
  public boolean transaction(final TransactionScope txBlock, final boolean joinCurrentTransaction) {
    return transaction(txBlock, joinCurrentTransaction,
        configuration.getValueAsInteger(GlobalConfiguration.TX_RETRIES), null,
        null);
  }

  @Override
  public boolean transaction(final TransactionScope txBlock, final boolean joinCurrentTransaction, final int attempts) {
    return transaction(txBlock, joinCurrentTransaction, attempts, null, null);
  }

  @Override
  public boolean transaction(final TransactionScope txBlock, final boolean joinCurrentTransaction, int attempts,
                             final OkCallback ok, final ErrorCallback error) {
    checkDatabaseIsOpen();
    if (txBlock == null)
      throw new IllegalArgumentException("Transaction block is null");

    ArcadeDBException lastException = null;

    if (attempts < 1)
      attempts = 1;

    for (int retry = 0; retry < attempts; ++retry) {
      boolean createdNewTx = true;
      try {
        if (joinCurrentTransaction && isTransactionActive())
          createdNewTx = false;
        else
          begin();

        txBlock.execute();

        if (createdNewTx)
          commit();

        if (ok != null)
          ok.call();

        return createdNewTx;

      } catch (final NeedRetryException | DuplicatedKeyException e) {
        // #661 (issue #7030 ported the guard here from LocalDatabase.transaction()): when we joined a
        // transaction owned by the caller (createdNewTx == false) we must NOT retry here. The retry would
        // re-run the block inside the caller's still-open transaction, which then accumulates the partial
        // effects of the failed attempt on top of the ones the caller made before the call, without ever
        // being told; and re-running against the same conflicted state cannot succeed anyway. Propagate the
        // exception so the real transaction owner retries the whole logical unit with fresh bindings.
        if (!createdNewTx)
          throw e;

        // RETRY
        lastException = e;
        // Close the server-side transaction before the next attempt: leaving it open keeps its locks until the
        // server times it out, so attempt N+1 would contend with the locks of attempt N and be MORE likely to
        // need a retry, not less (issue #7030). A failure raised by commit() has already ended the session
        // (commit() clears the session id in its finally), so this only fires when the block itself failed.
        rollbackQuietly();
        setSessionId(null);
        // The tx (server-side) is gone: reset records created in it so a retry/re-save inserts cleanly (issue #4562)
        resetCreatedRecordsIdentity();

        if (error != null)
          error.call(e);

      } catch (final Exception e) {
        // Same as above: the transaction this attempt left open on the server is never going to be committed,
        // so release it now instead of holding its locks until the server-side timeout (issue #7030).
        rollbackQuietly();
        setSessionId(null);
        resetCreatedRecordsIdentity();

        if (error != null)
          error.call(e);

        throw e;
      }
    }

    throw lastException;
  }

  public boolean isTransactionActive() {
    return getSessionId() != null;
  }

  @Override
  public int getNestedTransactions() {
    return isTransactionActive() ? 1 : 0;
  }

  @Override
  public RemoteTransactionExplicitLock acquireLock() {
    if (explicitLock == null)
      explicitLock = new RemoteTransactionExplicitLock(this);

    return explicitLock;
  }

  /**
   * Returns a builder for configuring a remote batch graph import.
   * The builder mirrors the server-side GraphBatch.Builder parameters.
   * <p>
   * The load travels over the same protocol as the connection it is started from: this class sends it as JSONL
   * to {@code POST /api/v1/batch}, while a subclass speaking another protocol overrides this method to return a
   * builder for its own loader - {@code RemoteGrpcDatabase} returns one backed by the {@code GraphBatchLoad}
   * streaming RPC (issue #6070). Every option below means the same thing either way.
   *
   * @return a new {@link RemoteGraphBatch.Builder}
   */
  public RemoteGraphBatch.Builder batch() {
    checkDatabaseIsOpen();
    return new RemoteGraphBatch.Builder(this);
  }

  @Override
  public void begin() {
    begin(transactionIsolationLevel);
  }

  @Override
  public void begin(final Database.TRANSACTION_ISOLATION_LEVEL isolationLevel) {
    checkDatabaseIsOpen();
    if (getSessionId() != null)
      throw new TransactionException("Transaction already begun");

    txCreatedRecords.clear();

    // For STICKY strategy: pin to a concrete cluster member before the HTTP call so
    // that begin, command, and commit all reach the same physical node. Prefer the
    // leader (already resolved from the cluster topology) to avoid an extra LB hop.
    if (getConnectionStrategy() == CONNECTION_STRATEGY.STICKY)
      setStickyTransactionServer(resolveStickyTargetServer());

    try {
      final JSONObject jsonRequest = new JSONObject().put("isolationLevel", isolationLevel);
      String payload = getRequestPayload(jsonRequest);

      HttpRequest request = createRequestBuilder("POST", getUrl("begin", databaseName)).POST(
          HttpRequest.BodyPublishers.ofString(payload)).header("Content-Type", "application/json").build();

      HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

      if (response.statusCode() != 204) {
        final Exception detail = manageException(response, "begin transaction");
        throw new TransactionException("Error on transaction begin", detail);
      }

      captureCommitIndexHeader(response);
      setSessionId(response.headers().firstValue(ARCADEDB_SESSION_ID).orElse(null));
    } catch (final Exception e) {
      throw new TransactionException("Error on transaction begin", e);
    } finally {
      if (getSessionId() == null)
        setStickyTransactionServer(null);
    }
  }

  // Prefer the leader (concrete pod) over currentServer (typically the LB hostname).
  Pair<String, Integer> resolveStickyTargetServer() {
    final Pair<String, Integer> leader = getLeaderServer();
    return leader != null ? leader : new Pair<>(currentServer, currentPort);
  }

  public void commit() {
    checkDatabaseIsOpen();
    stats.writeTx.incrementAndGet();

    if (getSessionId() == null)
      throw new TransactionException("Transaction not begun");

    boolean committed = false;
    try {
      HttpRequest request =
          createRequestBuilder("POST", getUrl("commit", databaseName)).POST(HttpRequest.BodyPublishers.noBody())
              .build();

      HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

      if (response.statusCode() != 204) {
        final Exception detail = manageException(response, "commit transaction");

        if (detail instanceof DuplicatedKeyException || detail instanceof ConcurrentModificationException)
          // SUPPORT RETRY
          throw detail;

        throw new TransactionException("Error on transaction commit", detail);
      }
      captureCommitIndexHeader(response);
      committed = true;
    } catch (final DuplicatedKeyException | ConcurrentModificationException e) {
      throw e;
    } catch (final Exception e) {
      throw new TransactionException("Error on transaction commit", e);
    } finally {
      if (committed)
        // SUCCESSFUL COMMIT: THE ASSIGNED RIDs ARE DURABLE, JUST DROP THE TRACKING
        txCreatedRecords.clear();
      else
        // FAILED COMMIT = SERVER-SIDE ROLLBACK: RESET CREATED RECORDS SO THEY CAN BE CLEANLY RE-INSERTED (ISSUE #4562)
        resetCreatedRecordsIdentity();
      setSessionId(null);
    }
  }

  public void rollback() {
    checkDatabaseIsOpen();
    stats.txRollbacks.incrementAndGet();

    if (getSessionId() == null)
      throw new TransactionException("Transaction not begun");

    try {
      HttpRequest request =
          createRequestBuilder("POST", getUrl("rollback", databaseName)).POST(HttpRequest.BodyPublishers.noBody())
              .build();

      HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

      if (response.statusCode() != 204) {
        final Exception detail = manageException(response, "rollback transaction");
        throw new TransactionException("Error on transaction rollback", detail);
      }
      captureCommitIndexHeader(response);
    } catch (final Exception e) {
      throw new TransactionException("Error on transaction rollback", e);
    } finally {
      resetCreatedRecordsIdentity();
      setSessionId(null);
    }
  }

  /**
   * Rolls back the server-side transaction, if one is still open, without ever throwing. Used on the failure paths
   * of {@link #transaction(TransactionScope, boolean, int, OkCallback, ErrorCallback)}, where the exception being
   * handled is the one the caller has to see: a rollback that fails on its way out (the session is already gone
   * server-side, the connection is broken) must not replace it, and there is nothing left to do about it anyway
   * since the server releases the transaction on its own timeout. Issue #7030.
   */
  private void rollbackQuietly() {
    if (!isTransactionActive())
      return;

    try {
      rollback();
    } catch (final Exception e) {
      LogManager.instance()
          .log(this, Level.FINE, "Error on rolling back the transaction of a failed attempt (ignored)", e);
    }
  }

  /**
   * Captures the {@code X-ArcadeDB-Commit-Index} response header on raw {@link #begin()}/{@link #commit()}/
   * {@link #rollback()} sends, which bypass {@link RemoteHttpComponent#httpCommand} and its bookmark capture
   * (issue #5845). The server emits this header on every begin/commit/rollback response once the database
   * has an applied index, so all three call sites can carry the just-committed bookmark forward for the next
   * {@link ReadConsistency#READ_YOUR_WRITES} read.
   */
  void captureCommitIndexHeader(final HttpResponse<String> response) {
    response.headers().firstValue("X-ArcadeDB-Commit-Index").ifPresent(val -> {
      try {
        updateLastCommitIndex(Long.parseLong(val));
      } catch (final NumberFormatException ignored) {
        // server sent an invalid header; ignore
      }
    });
  }

  /**
   * Tracks a record created in the current transaction so its identity can be reset if the transaction is rolled
   * back (issue #4562). Only meaningful inside an explicit transaction: outside one each save is auto-committed and
   * the assigned RID is durable.
   */
  protected void trackCreatedRecord(final MutableDocument record) {
    if (getSessionId() != null)
      txCreatedRecords.add(record);
  }

  /**
   * Resets to provisional the identity of every record created in the (now rolled-back) transaction, so re-saving the
   * same in-memory object cleanly inserts a new record instead of being treated as an update of a missing record.
   */
  protected void resetCreatedRecordsIdentity() {
    for (final MutableDocument r : txCreatedRecords)
      r.setIdentity(null);
    txCreatedRecords.clear();
  }

  @Override
  public long countBucket(final String bucketName) {
    checkDatabaseIsOpen();
    stats.countBucket.incrementAndGet();
    return ((Number) ((ResultSet) databaseCommand("query", "sql",
        "select count(*) as count from bucket:" + bucketName, null, false,
        (connection, response) -> createResultSet(response))).nextIfAvailable().getProperty("count")).longValue();
  }

  @Override
  public long countType(final String typeName, final boolean polymorphic) {
    checkDatabaseIsOpen();
    stats.countType.incrementAndGet();
    final String appendix = polymorphic ? "" : " where @type = '" + typeName + "'";
    return ((Number) ((ResultSet) databaseCommand("query", "sql",
        "select count(*) as count from " + typeName + appendix, null,
        false, (connection, response) -> createResultSet(response))).nextIfAvailable().getProperty("count")).longValue();
  }

  public Record lookupByRID(final RID rid) {
    stats.readRecord.incrementAndGet();
    if (rid == null)
      throw new IllegalArgumentException("Record is null");

    return lookupByRID(rid, true);
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

  @Override
  public Record lookupByRID(final RID rid, final boolean loadContent) {
    checkDatabaseIsOpen();
    stats.readRecord.incrementAndGet();
    if (rid == null)
      throw new IllegalArgumentException("Record is null");

    final ResultSet result = query("sql", "select from " + rid);
    if (!result.hasNext())
      throw new RecordNotFoundException("Record " + rid + " not found", rid);

    final Record record = result.next().getRecord().get();
    if (record == null)
      throw new RecordNotFoundException("Record " + rid + " not found", rid);

    return record;
  }

  @Override
  public void deleteRecord(final Record record) {
    checkDatabaseIsOpen();
    stats.deleteRecord.incrementAndGet();

    if (record.getIdentity() == null)
      throw new IllegalArgumentException("Cannot delete a non persistent record");

    command("SQL", "delete from " + record.getIdentity());
  }

  @Override
  public Iterator<Record> iterateType(final String typeName, final boolean polymorphic) {
    String query = "select from `" + typeName + "`";
    if (!polymorphic)
      query += " where @type = '" + typeName + "'";

    final ResultSet resultSet = query("sql", query);
    return new Iterator<>() {
      @Override
      public boolean hasNext() {
        return resultSet.hasNext();
      }

      @Override
      public Record next() {
        return resultSet.next().getElement().get();
      }
    };
  }

  @Override
  public Iterator<Record> iterateBucket(final String bucketName) {
    final ResultSet resultSet = query("sql", "select from bucket:`" + bucketName + "`");
    return new Iterator<>() {
      @Override
      public boolean hasNext() {
        return resultSet.hasNext();
      }

      @Override
      public Record next() {
        return resultSet.next().getElement().get();
      }
    };
  }

  @Override
  public ResultSet command(final String language, final String command, final Map<String, Object> params) {
    return command(language, command, null, params);
  }

  @Override
  public ResultSet command(final String language, final String command, final ContextConfiguration configuration,
                           final Object... args) {
    return command(language, command, args);
  }

  @Override
  public ResultSet command(final String language, final String command, final ContextConfiguration configuration,
                           final Map<String, Object> params) {
    checkDatabaseIsOpen();
    stats.commands.incrementAndGet();

    return (ResultSet) databaseCommand("command", language, command, params, true,
        (connection, response) -> createResultSet(response));
  }

  @Override
  public ResultSet command(final String language, final String command) {
    return command(language, command, new HashMap<>());
  }

  @Override
  public ResultSet command(final String language, final String command, final Object... args) {
    checkDatabaseIsOpen();
    stats.commands.incrementAndGet();

    final Map<String, Object> params = mapArgs(args);
    return (ResultSet) databaseCommand("command", language, command, params, true,
        (connection, response) -> createResultSet(response));
  }

  @Override
  public ResultSet query(final String language, final String query) {
    return query(language, query, new HashMap<>());
  }

  @Override
  public ResultSet query(final String language, final String query, final Object... args) {
    checkDatabaseIsOpen();
    stats.queries.incrementAndGet();

    final Map<String, Object> params = mapArgs(args);
    return (ResultSet) databaseCommand("query", language, query, params, false,
        (connection, response) -> createResultSet(response));
  }

  @Override
  public ResultSet query(final String language, final String query, final Map<String, Object> params) {
    checkDatabaseIsOpen();
    stats.commands.incrementAndGet();

    return (ResultSet) databaseCommand("query", language, query, params, false,
        (connection, response) -> createResultSet(response));
  }

  /**
   * @deprecated use {@link #command(String, String, Object...)} instead
   */
  @Deprecated
  @Override
  public ResultSet execute(final String language, final String command, final Object... args) {
    checkDatabaseIsOpen();
    stats.commands.incrementAndGet();

    final Map<String, Object> params = mapArgs(args);
    return (ResultSet) databaseCommand("command", language, command, params, false,
        (connection, response) -> createResultSet(response));
  }

  /**
   * Runs a query and returns a {@link ResultSet} that reads the rows off the connection as they arrive, instead
   * of the buffered {@link #query(String, String, Map)}, which waits for the server to serialize the entire
   * result before it can return anything (issue #7306).
   * <p>
   * Both ends are streamed: the server holds one row at a time while it writes, and this driver holds one row at
   * a time while it reads. That is a memory property, not only a latency one - it is what makes a result larger
   * than either heap iterable at all.
   * <p>
   * The returned {@link ResultSet} owns an open HTTP connection until it is exhausted or closed, so it belongs in
   * a try-with-resources. It is also single-pass: {@code reset()} is not supported, and the rows are gone once
   * read. A caller that needs the whole result in memory, or the {@code explain} / {@code stats} envelope
   * properties, wants {@link #query(String, String, Map)} instead - which is unchanged.
   *
   * @param params named parameters, or an empty map
   *
   * @throws RemoteException if the server answers anything other than 200, or the stream ends without its
   *                         trailer
   */
  public ResultSet queryStream(final String language, final String query, final Map<String, Object> params) {
    checkDatabaseIsOpen();
    stats.queries.incrementAndGet();
    return streamingCommand("query", language, query, params);
  }

  /**
   * Positional-parameter form of {@link #queryStream(String, String, Map)}.
   */
  public ResultSet queryStream(final String language, final String query, final Object... args) {
    return queryStream(language, query, mapArgs(args));
  }

  /**
   * Streaming counterpart of {@link #command(String, String, Map)}, for a command whose result is a row stream
   * too large to buffer. Same contract and same caveats as {@link #queryStream(String, String, Map)}.
   */
  public ResultSet commandStream(final String language, final String command, final Map<String, Object> params) {
    checkDatabaseIsOpen();
    stats.commands.incrementAndGet();
    return streamingCommand("command", language, command, params);
  }

  /**
   * kNN search over a dense {@code LSM_VECTOR} or sparse {@code LSM_SPARSE_VECTOR} index (issue #7306).
   * <p>
   * The request and response shapes, and every bound the server enforces on them, are documented under
   * {@code POST /api/v1/vector/{database}/search} in the OpenAPI document, and are shared with the gRPC
   * {@code VectorSearch} RPC and the MCP {@code vector_search} tool. ArcadeDB does not generate embeddings: the
   * caller supplies {@code queryVector}.
   *
   * @param request at least {@code indexName}, {@code queryVector} and {@code k}
   *
   * @return the server's response object, carrying {@code results} plus the {@code scoring}, {@code count} and
   *         {@code truncated} accounting
   */
  public JSONObject vectorSearch(final JSONObject request) {
    return vectorOperation("search", request);
  }

  /**
   * Fused vector + full-text + graph-expansion search, documented under
   * {@code POST /api/v1/vector/{database}/hybrid}. Same sharing as {@link #vectorSearch(JSONObject)}.
   *
   * @param request at least {@code vectorIndexName}, {@code queryVector} and {@code k}
   */
  public JSONObject hybridSearch(final JSONObject request) {
    return vectorOperation("hybrid", request);
  }

  /**
   * Full-text search over a {@code FULL_TEXT} index, documented under
   * {@code POST /api/v1/vector/{database}/fulltext}. Same sharing as {@link #vectorSearch(JSONObject)}.
   *
   * @param request at least {@code queryText}, plus {@code indexName} or {@code typeName} to address the index
   */
  public JSONObject fullTextSearch(final JSONObject request) {
    return vectorOperation("fulltext", request);
  }

  private JSONObject vectorOperation(final String operation, final JSONObject request) {
    checkDatabaseIsOpen();
    if (request == null)
      throw new IllegalArgumentException("The search request cannot be null");
    stats.queries.incrementAndGet();

    try {
      final HttpRequest httpRequest = addReadConsistencyHeaders(createRequestBuilder("POST",
          getUrl("vector/" + databaseName + "/" + operation)))
          .method("POST", HttpRequest.BodyPublishers.ofString(request.toString()))
          .header("Content-Type", "application/json")
          .build();

      // Through the same watchdog every other request uses, so a server that accepts the connection and then
      // stops answering is bounded by the configured timeout rather than by the JDK's default of none.
      final HttpResponse<String> response = sendWithWatchdog(httpRequest);
      if (response.statusCode() != 200)
        throw asRuntime(manageException(response, "vector " + operation), "vector " + operation);

      return new JSONObject(response.body());
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RemoteException("Request interrupted", e);
    } catch (final RuntimeException e) {
      // Unchanged, for the reason RemoteHttpComponent.httpCommand gives at its own generic clause: manageException
      // reconstructs the server's exception type, and SecurityException and NoSuchElementException are neither
      // RemoteException nor ArcadeDBException. Catching only those two supertypes buried a denied vector search as
      // a generic RemoteException, so a caller could not tell authorization from transport (claude-review).
      throw e;
    } catch (final Exception e) {
      throw new RemoteException("Error on executing vector " + operation, e);
    }
  }

  /**
   * Rethrows what {@link #manageException} mapped a failed response onto, unchanged when it is already an
   * unchecked exception. Unchanged is the point: the mapping reconstructs the engine's own exception type and
   * carries the server's explanation in its message, and re-wrapping it would hide both behind a generic
   * "error on executing" - which is exactly what makes a bound crossed on one surface unreadable on another.
   * This mirrors what the buffered {@code httpCommand} path does with the same value.
   */
  private static RuntimeException asRuntime(final Exception mapped, final String operation) {
    if (mapped instanceof final RuntimeException runtime)
      return runtime;
    return new RemoteException("Error on executing " + operation, mapped);
  }

  /**
   * Issues one query/command and hands back its NDJSON body as a lazily-read {@link ResultSet}.
   * <p>
   * Deliberately a single attempt against the currently selected server, unlike the buffered
   * {@code httpCommand} path with its failover loop: a stream cannot be replayed once bytes have been delivered,
   * and silently re-running a command on a second server after the first one failed mid-stream would hand the
   * caller two partial results glued together. A failure before the stream starts still surfaces as a
   * {@link RemoteException} naming the cause, which is what a caller can act on.
   */
  private ResultSet streamingCommand(final String operation, final String language, final String command,
      final Map<String, Object> params) {
    final JSONObject jsonRequest = new JSONObject();
    if (language != null)
      jsonRequest.put("language", language);
    jsonRequest.put("command", command);
    jsonRequest.put("serializer", "record");
    // Same opt-in the buffered path makes (issue #5812): this driver rebuilds the exact Java type of a
    // projection/aggregate column from the @props hint, so it has to ask for it.
    jsonRequest.put("typeHints", true);
    jsonRequest.put("retries", txRetries);
    final Integer maxRows = getMaxResultRows();
    if (maxRows != null)
      jsonRequest.put("limit", maxRows);
    if (params != null && !params.isEmpty())
      jsonRequest.put("params", new JSONObject(params));

    InputStream body = null;
    try {
      final HttpRequest request = addReadConsistencyHeaders(
          createRequestBuilder("POST", getUrl(operation + "/" + databaseName)))
          .method("POST", HttpRequest.BodyPublishers.ofString(jsonRequest.toString()))
          .header("Content-Type", "application/json")
          .header("Accept", NDJSON_CONTENT_TYPE)
          .build();

      final HttpResponse<InputStream> response = httpClient.send(request, HttpResponse.BodyHandlers.ofInputStream());
      body = response.body();

      if (response.statusCode() != 200) {
        // The failure body is small and already complete: read it so the standard error mapping can name the
        // exception type, exactly as the buffered path does.
        final String errorBody = new String(body.readAllBytes(), StandardCharsets.UTF_8);
        body.close();
        body = null;
        throw asRuntime(manageException(response.statusCode(), errorBody, command), "streamed " + operation);
      }

      // A server that predates #7306 ignores the Accept header and answers the buffered envelope with a 200.
      // Without this check the driver would hand that body to the NDJSON reader and fail somewhere in the middle
      // of it with a parse error, which says nothing about the actual cause. Checking the type the server
      // committed to says it once, up front.
      final String contentType = response.headers().firstValue("content-type").orElse("");
      if (!contentType.toLowerCase(Locale.ROOT).contains(NDJSON_CONTENT_TYPE)) {
        body.close();
        body = null;
        throw new RemoteException("The server answered '" + (contentType.isEmpty() ? "no content type" : contentType)
            + "' instead of '" + NDJSON_CONTENT_TYPE + "': it does not support streamed queries. Use query() "
            + "instead, or upgrade the server");
      }

      final BufferedReader reader = new BufferedReader(new InputStreamReader(body, StandardCharsets.UTF_8));
      body = null; // ownership passes to the ResultSet, which closes it
      return new RemoteStreamingResultSet(reader, this::json2Result, maxRows == null);

    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RemoteException("Request interrupted", e);
    } catch (final RemoteException | ArcadeDBException e) {
      throw e;
    } catch (final IOException e) {
      throw new RemoteException("Error on executing streamed " + operation, e);
    } finally {
      if (body != null)
        try {
          body.close();
        } catch (final IOException ignored) {
          // Nothing left to do: the request already failed, and the connection is being discarded anyway.
        }
    }
  }

  public Database.TRANSACTION_ISOLATION_LEVEL getTransactionIsolationLevel() {
    return transactionIsolationLevel;
  }

  public void setTransactionIsolationLevel(final Database.TRANSACTION_ISOLATION_LEVEL transactionIsolationLevel) {
    this.transactionIsolationLevel = transactionIsolationLevel;
  }

  public ReadConsistency getReadConsistency() {
    return readConsistency;
  }

  public void setReadConsistency(final ReadConsistency readConsistency) {
    if (readConsistency == null)
      throw new IllegalArgumentException("readConsistency cannot be null");
    this.readConsistency = readConsistency;
  }

  public long getLastCommitIndex() {
    return lastCommitIndex.get();
  }

  void updateLastCommitIndex(final long newValue) {
    lastCommitIndex.accumulateAndGet(newValue, Math::max);
  }

  public int getElectionRetryCount() {
    return electionRetryCount;
  }

  public void setElectionRetryCount(final int electionRetryCount) {
    this.electionRetryCount = electionRetryCount;
  }

  public long getElectionRetryDelayMs() {
    return electionRetryDelayMs;
  }

  public void setElectionRetryDelayMs(final long electionRetryDelayMs) {
    this.electionRetryDelayMs = electionRetryDelayMs;
  }

  @Override
  public String toString() {
    return databaseName;
  }

  private Object databaseCommand(final String operation, final String language, final String payloadCommand,
                                 final Map<String, Object> params, final boolean requiresLeader,
                                 final Callback callback) {
    checkDatabaseIsOpen();
    return httpCommand("POST", databaseName, operation, language, payloadCommand, params, requiresLeader, true,
        callback);
  }

  /**
   * Returns the long-running maintenance operations (CHECK DATABASE, ...) currently in progress on the
   * connected server for this database (issue #5372), one JSON object per operation carrying
   * {@code operation}, {@code stepName}, {@code stepIndex}, {@code totalSteps}, {@code done}, {@code total}
   * and {@code percentage} (-1 when the step total is unknown). Safe to poll at any frequency: the server side
   * answers from a lock-free in-memory snapshot without touching the database.
   */
  public List<JSONObject> getProgress() {
    checkDatabaseIsOpen();
    try {
      final HttpRequest request = createRequestBuilder("GET", getUrl("progress", databaseName)).GET().build();
      final HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

      if (response.statusCode() != 200)
        throw new RemoteException("Error on requesting operation progress", manageException(response, "progress"));

      final JSONArray result = new JSONObject(response.body()).getJSONArray("result");
      final List<JSONObject> operations = new ArrayList<>(result.length());
      for (int i = 0; i < result.length(); i++)
        operations.add(result.getJSONObject(i));
      return operations;
    } catch (final RemoteException | SecurityException e) {
      throw e;
    } catch (final Exception e) {
      throw new RemoteException("Error on requesting operation progress", e);
    }
  }

  // ---------------------------------------------------------------------------------------------------------
  // Time series API (issue #7305)
  //
  // The server has had /ts/write, /ts/query and /ts/latest for a long time; this client could not reach any of
  // them, so a Java application had to hand-roll HTTP to use its own database's time-series store. These four
  // methods close that, in a shape RemoteGrpcDatabase overrides with the equivalent gRPC RPCs - so the same
  // application code runs over either protocol, and the two are testable against each other.
  // ---------------------------------------------------------------------------------------------------------

  /**
   * Ingests time-series samples.
   * <p>
   * Not atomic: each measurement's batch commits its own shard transaction as it is appended, so a summary
   * reporting drops is a partial write and not a rollback - see {@link TimeSeriesWriteSummary}. A caller that
   * needs to know nothing was dropped checks {@link TimeSeriesWriteSummary#isComplete()}.
   *
   * @param points the samples; timestamps are epoch milliseconds
   *
   * @return what was written and what was dropped
   */
  public TimeSeriesWriteSummary timeSeriesWrite(final List<TimeSeriesPoint> points) {
    checkDatabaseIsOpen();
    if (points == null || points.isEmpty())
      return TimeSeriesWriteSummary.empty();

    final StringBuilder body = new StringBuilder(points.size() * 64);
    for (final TimeSeriesPoint point : points)
      LineProtocolWriter.appendLine(body, point.type(), point.tags(), point.fields(), point.timestampMs());

    try {
      // precision=ms is not optional: the endpoint defaults to nanoseconds, which would divide every timestamp
      // LineProtocolWriter emits by a million.
      final HttpRequest request = createRequestBuilder("POST",
          getUrl("ts", databaseName) + "/write?precision=ms")
          .POST(HttpRequest.BodyPublishers.ofString(body.toString()))
          .header("Content-Type", "text/plain")
          .build();

      final HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

      // Captured unconditionally, not only on success: a partial write's already-appended samples are durable,
      // so a READ_YOUR_WRITES client that skipped the bookmark on the 400 would silently miss them - the same
      // reasoning sendBatch applies to a partially committed batch.
      captureCommitIndexHeader(response);

      if (response.statusCode() == 204)
        return new TimeSeriesWriteSummary(points.size(), points.size(), 0, List.of(), List.of(), List.of());

      if (response.statusCode() == 400) {
        final JSONObject error = new JSONObject(response.body());
        // A partial write and a rejected request share the 400. The counts are what tells them apart: a
        // rejected request (empty body, missing database) carries none.
        if (error.has("written") && error.has("dropped"))
          return new TimeSeriesWriteSummary(points.size(), error.getLong("written"), error.getLong("dropped"),
              stringList(error.getJSONArray("unknownTypes", null)), stringList(error.getJSONArray("nonTimeSeriesTypes", null)),
              stringList(error.getJSONArray("unavailableTypes", null)));
      }

      throw new RemoteException("Error on time series write", manageException(response, "ts write"));
    } catch (final RemoteException | SecurityException e) {
      throw e;
    } catch (final Exception e) {
      throw new RemoteException("Error on time series write", e);
    }
  }

  /**
   * Ingests time-series samples in chunks, for a producer whose whole batch should not be materialized as one
   * request. Over HTTP this is one request per chunk, whose summaries are added together; over gRPC
   * ({@code RemoteGrpcDatabase}) it is a single client-streaming call.
   * <p>
   * Because each chunk is its own write, a failure part-way leaves the earlier chunks durable - the same
   * partial-write contract a single call has, one level up.
   *
   * @param points    the samples to ingest
   * @param chunkSize samples per chunk; must be positive
   */
  public TimeSeriesWriteSummary timeSeriesWriteStream(final Iterable<TimeSeriesPoint> points, final int chunkSize) {
    checkDatabaseIsOpen();
    if (chunkSize <= 0)
      throw new IllegalArgumentException("chunkSize must be positive");

    TimeSeriesWriteSummary summary = TimeSeriesWriteSummary.empty();
    final List<TimeSeriesPoint> chunk = new ArrayList<>(chunkSize);
    for (final TimeSeriesPoint point : points) {
      chunk.add(point);
      if (chunk.size() == chunkSize) {
        summary = summary.plus(timeSeriesWrite(chunk));
        chunk.clear();
      }
    }
    if (!chunk.isEmpty())
      summary = summary.plus(timeSeriesWrite(chunk));
    return summary;
  }

  /**
   * Reads samples from a time-series type, raw or aggregated into fixed-interval buckets according to whether
   * {@code query} states an aggregation.
   */
  public TimeSeriesQueryResult timeSeriesQuery(final TimeSeriesQuery query) {
    checkDatabaseIsOpen();

    final JSONObject payload = new JSONObject();
    payload.put("type", query.getType());
    if (query.getFromTimestamp() != null)
      payload.put("from", query.getFromTimestamp().longValue());
    if (query.getToTimestamp() != null)
      payload.put("to", query.getToTimestamp().longValue());
    if (!query.getFields().isEmpty())
      payload.put("fields", new JSONArray(query.getFields()));
    if (!query.getTags().isEmpty()) {
      final JSONObject tags = new JSONObject();
      for (final Map.Entry<String, Object> tag : query.getTags().entrySet())
        tags.put(tag.getKey(), tag.getValue());
      payload.put("tags", tags);
    }
    if (query.getLimit() > 0)
      payload.put("limit", query.getLimit());
    if (query.isAggregated()) {
      final JSONArray requests = new JSONArray();
      for (final TimeSeriesQuery.Aggregation aggregation : query.getAggregations()) {
        final JSONObject request = new JSONObject();
        request.put("field", aggregation.field());
        request.put("type", aggregation.type().name());
        request.put("alias", aggregation.resolvedAlias());
        requests.put(request);
      }
      final JSONObject aggregation = new JSONObject();
      aggregation.put("bucketInterval", query.getBucketIntervalMs());
      aggregation.put("requests", requests);
      payload.put("aggregation", aggregation);
    }

    final JSONObject response = postToTimeSeriesEndpoint("query", payload, "ts query");

    if (query.isAggregated()) {
      final JSONArray aggregations = response.getJSONArray("aggregations", null);
      final JSONArray buckets = response.getJSONArray("buckets", null);
      final List<TimeSeriesBucket> parsed = new ArrayList<>(buckets == null ? 0 : buckets.length());
      if (buckets != null)
        for (int i = 0; i < buckets.length(); i++) {
          final JSONObject bucket = buckets.getJSONObject(i);
          parsed.add(new TimeSeriesBucket(bucket.getLong("timestamp"), jsonValues(bucket.getJSONArray("values"))));
        }
      return new TimeSeriesQueryResult(response.getString("type"), List.of(), List.of(), stringList(aggregations),
          parsed, false);
    }

    final JSONArray rows = response.getJSONArray("rows", null);
    final List<Object[]> parsed = new ArrayList<>(rows == null ? 0 : rows.length());
    if (rows != null)
      for (int i = 0; i < rows.length(); i++)
        parsed.add(jsonValues(rows.getJSONArray(i)));

    return new TimeSeriesQueryResult(response.getString("type"), stringList(response.getJSONArray("columns", null)),
        parsed, List.of(), List.of(), response.getBoolean("truncated", false));
  }

  /** The newest sample of {@code typeName}, across every series. */
  public TimeSeriesLatestResult timeSeriesLatest(final String typeName) {
    return timeSeriesLatest(typeName, null, null);
  }

  /**
   * The newest sample of {@code typeName} among the series whose {@code tagName} equals {@code tagValue}.
   * <p>
   * One predicate, not a map, because that is what both protocols express identically: the HTTP endpoint's
   * {@code tag} query parameter carries a single {@code name:value} pair and ignores any repeat, a contract
   * {@code TimeSeriesApiSpecTest} pins. The gRPC {@code TimeSeriesLatest} RPC does accept a whole filter map -
   * a gRPC client using the proto directly can use it - and closing that asymmetry on the HTTP side is tracked
   * separately (see the follow-up named in the PR for issue #7305).
   *
   * @param tagName  the tag column, or {@code null} to select every series
   * @param tagValue the value that tag must equal
   */
  public TimeSeriesLatestResult timeSeriesLatest(final String typeName, final String tagName,
      final Object tagValue) {
    checkDatabaseIsOpen();

    final StringBuilder url = new StringBuilder(getUrl("ts", databaseName)).append("/latest?type=")
        .append(URLEncoder.encode(typeName, DatabaseFactory.getDefaultCharset()));
    if (tagName != null && !tagName.isBlank())
      url.append("&tag=").append(URLEncoder.encode(tagName + ":" + tagValue, DatabaseFactory.getDefaultCharset()));

    try {
      final HttpRequest request = createRequestBuilder("GET", url.toString()).GET().build();
      final HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

      if (response.statusCode() != 200)
        throw new RemoteException("Error on time series latest", manageException(response, "ts latest"));

      final JSONObject body = new JSONObject(response.body());
      final Object[] latest = body.isNull("latest") ? null : jsonValues(body.getJSONArray("latest"));
      return new TimeSeriesLatestResult(body.getString("type"), stringList(body.getJSONArray("columns", null)), latest);
    } catch (final RemoteException | SecurityException e) {
      throw e;
    } catch (final Exception e) {
      throw new RemoteException("Error on time series latest", e);
    }
  }

  private JSONObject postToTimeSeriesEndpoint(final String endpoint, final JSONObject payload,
      final String operation) {
    try {
      final HttpRequest request = createRequestBuilder("POST", getUrl("ts", databaseName) + "/" + endpoint)
          .POST(HttpRequest.BodyPublishers.ofString(payload.toString()))
          .header("Content-Type", "application/json")
          .build();

      final HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
      if (response.statusCode() != 200)
        throw new RemoteException("Error on time series " + endpoint, manageException(response, operation));

      return new JSONObject(response.body());
    } catch (final RemoteException | SecurityException e) {
      throw e;
    } catch (final Exception e) {
      throw new RemoteException("Error on time series " + endpoint, e);
    }
  }

  /**
   * Reads a JSON array of sample values, turning JSON null into Java null. A value that stands for "no
   * measurement" - an absent MIN/MAX, a non-finite sample - arrives as JSON null and must not become the
   * string "null" or a zero.
   * <p>
   * Numbers keep whatever concrete {@link Number} the JSON parser chose for their text, so a caller comparing
   * against an embedded or gRPC result should compare numerically rather than by {@code equals}.
   */
  private static Object[] jsonValues(final JSONArray array) {
    final Object[] values = new Object[array.length()];
    for (int i = 0; i < values.length; i++)
      values[i] = array.isNull(i) ? null : array.get(i);
    return values;
  }

  private static List<String> stringList(final JSONArray array) {
    if (array == null || array.length() == 0)
      return List.of();
    final List<String> values = new ArrayList<>(array.length());
    for (int i = 0; i < array.length(); i++)
      values.add(array.getString(i));
    return values;
  }

  String getSessionId() {
    return sessionId;
  }

  protected void setSessionId(final String sessionId) {
    this.sessionId = sessionId;
    if (sessionId == null)
      setStickyTransactionServer(null);
  }

  HttpRequest.Builder createRequestBuilder(final String httpMethod, final String url) {
    HttpRequest.Builder builder = super.createRequestBuilder(httpMethod, url);

    if (getSessionId() != null)
      builder.header(ARCADEDB_SESSION_ID, getSessionId());

    return builder;
  }

  private String getUrl(final String command, final String databaseName) {
    return getUrl(command) + "/" + databaseName;
  }

  JSONObject sendBatch(final String content, final Map<String, String> queryParams) {
    return sendBatch(content, queryParams, null);
  }

  /**
   * Sends one bulk-load payload to {@code POST /api/v1/batch} and returns the load's summary object.
   * <p>
   * With a {@code onProgress} listener the request negotiates the streaming encoding of issue #7311
   * ({@code Accept: application/x-ndjson}) and the listener is handed every {@code progress} line as it arrives,
   * so a caller learns what the server has committed while the rest of its payload is still being read. The
   * object returned is the terminal {@code summary} line, which carries exactly the fields the buffered
   * encoding returns - so nothing downstream of this method has to know which encoding was used.
   * <p>
   * With a {@code null} listener nothing is negotiated and the buffered request goes out exactly as before.
   *
   * @param onProgress notified once per chunk acknowledgement, or {@code null} to send an unnegotiated request
   */
  JSONObject sendBatch(final String content, final Map<String, String> queryParams,
      final Consumer<JSONObject> onProgress) {
    checkDatabaseIsOpen();

    final StringBuilder urlBuilder = new StringBuilder(getUrl("batch", databaseName));
    if (queryParams != null && !queryParams.isEmpty()) {
      urlBuilder.append('?');
      boolean first = true;
      for (final Map.Entry<String, String> entry : queryParams.entrySet()) {
        if (!first)
          urlBuilder.append('&');
        urlBuilder.append(entry.getKey()).append('=').append(entry.getValue());
        first = false;
      }
    }

    try {
      final HttpRequest.Builder builder = createRequestBuilder("POST", urlBuilder.toString())
          .POST(HttpRequest.BodyPublishers.ofString(content))
          .header("Content-Type", NDJSON_CONTENT_TYPE);
      if (onProgress != null)
        builder.header("Accept", NDJSON_CONTENT_TYPE);
      final HttpRequest request = builder.build();

      if (onProgress != null)
        return readStreamedBatch(httpClient.send(request, HttpResponse.BodyHandlers.ofInputStream()), onProgress);

      final HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

      // GraphBatch commits internally every commitEvery records (issue #5862), so unlike a single
      // begin/commit/rollback a non-200 response here can still carry chunks the server already made
      // durable (see PostBatchHandler's partialCommit responses): the bookmark is captured unconditionally,
      // not only on success, or a READ_YOUR_WRITES client would silently miss the records that did commit.
      captureCommitIndexHeader(response);

      if (response.statusCode() != 200) {
        final Exception detail = manageException(response, "batch import");
        throw new DatabaseOperationException("Error on batch import", detail);
      }

      return new JSONObject(response.body());
    } catch (final DatabaseOperationException e) {
      throw e;
    } catch (final Exception e) {
      throw new DatabaseOperationException("Error on batch import", e);
    }
  }

  /**
   * Consumes the streaming answer of a bulk load: dispatches every {@code progress} line to the listener and
   * returns the terminal {@code summary} (issue #7311).
   * <p>
   * A server that answered with the buffered encoding after being asked for the stream is an older node, and is
   * read as the single object it is rather than fed to the line reader - the same fallback check
   * {@code query()} makes for the streaming query, and for the same reason: without it the driver would fail
   * somewhere in the middle of a body that is perfectly valid.
   * <p>
   * A stream that ends with neither {@code summary} nor {@code error} did not arrive whole - a dropped
   * connection, a proxy that cut it - and is raised as a failure rather than returned as a load with no
   * counters, which would look to the caller exactly like a load of an empty payload.
   */
  private JSONObject readStreamedBatch(final HttpResponse<InputStream> response,
      final Consumer<JSONObject> onProgress) throws IOException {

    final String contentType = response.headers().firstValue("Content-Type").orElse("");
    if (!contentType.toLowerCase(Locale.ROOT).contains(NDJSON_CONTENT_TYPE)) {
      try (final InputStream in = response.body()) {
        final String body = new String(in.readAllBytes(), DatabaseFactory.getDefaultCharset());
        if (response.statusCode() != 200)
          throw new DatabaseOperationException("Error on batch import: " + body);
        return new JSONObject(body);
      }
    }

    try (final BufferedReader reader = new BufferedReader(
        new InputStreamReader(response.body(), DatabaseFactory.getDefaultCharset()))) {
      for (String line = reader.readLine(); line != null; line = reader.readLine()) {
        if (line.isBlank())
          continue;
        final JSONObject event = new JSONObject(line);
        if (event.has("progress")) {
          onProgress.accept(event.getJSONObject("progress"));
          continue;
        }
        if (event.has("summary")) {
          final JSONObject summary = event.getJSONObject("summary");
          // The bookmark of issue #5862 rides in the terminal line on this encoding, because the response has
          // already started by the time the server knows it and a header set then would be dropped in silence.
          if (summary.has("commitIndex"))
            updateLastCommitIndex(summary.getLong("commitIndex"));
          return summary;
        }
        if (event.has("error")) {
          final JSONObject error = event.getJSONObject("error");
          if (error.has("commitIndex"))
            updateLastCommitIndex(error.getLong("commitIndex"));
          throw new DatabaseOperationException("Error on batch import (status " + error.getInt("status", 0) + "): "
              + error.getString("error", "no message") + ". The load is not atomic: "
              + error.getLong("verticesCreated", 0) + " vertices and " + error.getLong("edgesCreated", 0)
              + " edges were attempted before it failed, and the chunks before the failure are durable - "
              + "re-sending the whole payload would duplicate them");
        }
      }
    }

    throw new DatabaseOperationException(
        "The streamed batch answer ended without a summary or an error line, so the load did not complete and how "
            + "much of it was committed is unknown");
  }

  protected ResultSet createResultSet(final JSONObject response) {
    final ResultSet resultSet = new InternalResultSet();

    if (getMaxResultRows() == null && response.getBoolean("truncated", false))
      // The server dropped rows this driver never asked to drop: without this the caller would receive a
      // partial ResultSet indistinguishable from a complete one, and would silently disagree with the same
      // query executed on the embedded API (issue #5711). An application that set its own cap with
      // setMaxResultRows() asked for the truncation, so it is not warned about it - the same rule the server
      // applies to a request carrying its own 'limit', and what keeps a paging application out of the log.
      LogManager.instance().log(this, Level.WARNING,
          "The server truncated the result set to %d rows (its limit is %d): the returned result is incomplete. Add an explicit "
              + "LIMIT to the query, or raise the cap with RemoteDatabase.setMaxResultRows().",
          response.getInt("returned", -1), response.getInt("limit", -1));

    final JSONArray resultArray = response.getJSONArray("result");
    for (int i = 0; i < resultArray.length(); ++i) {
      final JSONObject result = resultArray.getJSONObject(i);
      ((InternalResultSet) resultSet).add(json2Result(result));
    }
    return resultSet;
  }

  protected Result json2Result(final JSONObject result) {
    final Record record = json2Record(result);
    if (record == null) {
      // Issue #4267: honor the per-column type hints emitted by JsonSerializer.serializeResult so
      // numeric aggregates like count(*) preserve their declared Java type (e.g. Long) instead of
      // collapsing to the JSONObject default of Integer when the value fits in 32 bits. Matches the
      // behavior of the gRPC client, which already routes the value through a typed channel.
      final Map<String, Object> map = result.toMap();
      final Map<String, ColumnTypeHint> propTypes = parsePropertyTypes((String) map.get(Property.PROPERTY_TYPES_PROPERTY));
      if (!propTypes.isEmpty() || map.containsKey(Property.PROPERTY_TYPES_PROPERTY)) {
        final Map<String, Object> converted = new LinkedHashMap<>(map.size());
        for (final Map.Entry<String, Object> entry : map.entrySet()) {
          final String fieldName = entry.getKey();
          if (Property.METADATA_PROPERTIES.contains(fieldName))
            continue;
          final ColumnTypeHint hint = propTypes.get(fieldName);
          Object value = entry.getValue();
          if (hint != null && value != null)
            value = convertWithHint(value, hint);
          converted.put(fieldName, value);
        }
        return new ResultInternal(converted);
      }
      return new ResultInternal(map);
    }

    return new ResultInternal(record);
  }

  /**
   * Restores a projection column value to its declared Java type using the per-column {@code @props}
   * hint. Temporal columns honor the configured date/datetime implementation (matching the schema
   * driven document path), and - issue #4849 - a LIST/MAP column carrying an element-type hint has
   * every item coerced to that element type, so a projected {@code List}/{@code Map} of temporals
   * reaches the caller as {@code List<LocalDateTime>}/{@code Map<String,LocalDateTime>} instead of a
   * raw container of epoch-millis {@code Long} values.
   */
  private Object convertWithHint(final Object value, final ColumnTypeHint hint) {
    if (hint.elementType() != null) {
      final Class<?> elementImplementation = javaImplementationForType(hint.elementType());
      if (value instanceof List<?> list) {
        final List<Object> converted = new ArrayList<>(list.size());
        for (final Object item : list)
          converted.add(item == null ? null : Type.convert(null, item, elementImplementation));
        return converted;
      }
      if (value instanceof Map<?, ?> map) {
        final Map<Object, Object> converted = new LinkedHashMap<>(map.size());
        for (final Map.Entry<?, ?> mapEntry : map.entrySet())
          converted.put(mapEntry.getKey(),
              mapEntry.getValue() == null ? null : Type.convert(null, mapEntry.getValue(), elementImplementation));
        return converted;
      }
    }
    return Type.convert(null, value, javaImplementationForType(hint.type()));
  }

  private Class<?> javaImplementationForType(final Type type) {
    if (type == Type.DATE)
      return serializer.getDateImplementation();
    if (type == Type.DATETIME)
      return serializer.getDateTimeImplementation();
    return type.getDefaultJavaType();
  }

  private static Map<String, ColumnTypeHint> parsePropertyTypes(final String propTypesAsString) {
    if (propTypesAsString == null || propTypesAsString.isEmpty())
      return Collections.emptyMap();

    final Map<String, ColumnTypeHint> propTypes = new HashMap<>();
    for (final String entry : propTypesAsString.split(",")) {
      final int sep = entry.lastIndexOf(':');
      if (sep <= 0 || sep == entry.length() - 1)
        continue;
      final String fieldName = entry.substring(0, sep);
      String typePart = entry.substring(sep + 1);

      // Issue #4849: an optional element-type id is encoded in parentheses for collection columns,
      // e.g. "dates:9(6)" -> LIST(9) of DATETIME(6).
      Type elementType = null;
      final int paren = typePart.indexOf('(');
      if (paren > 0 && typePart.endsWith(")")) {
        try {
          elementType = Type.getById((byte) Integer.parseInt(typePart.substring(paren + 1, typePart.length() - 1)));
        } catch (final NumberFormatException ignored) {
          // ignore a malformed element-type suffix and fall back to the column type only
        }
        typePart = typePart.substring(0, paren);
      }

      try {
        propTypes.put(fieldName, new ColumnTypeHint(Type.getById((byte) Integer.parseInt(typePart)), elementType));
      } catch (final NumberFormatException ignored) {
        // skip malformed entries rather than fail the whole result row
      }
    }
    return propTypes;
  }

  /**
   * Per-column type metadata parsed from the {@code @props} hint: the column {@link Type} and, for a
   * collection column, the optional element {@link Type} (issue #4849).
   */
  private record ColumnTypeHint(Type type, Type elementType) {
  }

  protected Record json2Record(final JSONObject result) {
    final Map<String, Object> map = result.toMap();

    if (map.containsKey(CAT_PROPERTY)) {
      final String cat = result.getString(CAT_PROPERTY);
      return switch (cat) {
        case "d" -> new RemoteImmutableDocument(this, map);
        case "v" -> new RemoteImmutableVertex(this, map);
        case "e" -> new RemoteImmutableEdge(this, map);
        default -> null; // Or throw an exception for unknown category
      };
    }
    return null;
  }

  protected RID saveRecord(final MutableDocument record) {
    stats.createRecord.incrementAndGet();

    RID rid = record.getIdentity();
    final JSONObject json = record.toJSON();
    json.remove(RID_PROPERTY);  // Remove @rid to avoid SQL parsing issues
    if (rid != null) {
      // SQL UPDATE silently matches zero records when the RID no longer exists (e.g. the record was deleted or the
      // transaction that created it was rolled back). Saving such a record must fail with RecordNotFoundException to be
      // consistent with the embedded engine and the gRPC remote (issue #4562).
      final ResultSet result = command("sql", "update " + rid + " content " + json);
      final long updated = result.hasNext() ? result.next().<Number>getProperty("count").longValue() : 0;
      if (updated == 0)
        throw new RecordNotFoundException("Record " + rid + " not found", rid);
    } else {
      final ResultSet result = command("sql", "insert into " + record.getTypeName() + " content " + json);
      rid = result.next().getIdentity().get();
      trackCreatedRecord(record);
    }
    return rid;
  }

  protected RID saveRecord(final MutableDocument record, final String bucketName) {
    stats.createRecord.incrementAndGet();

    RID rid = record.getIdentity();
    if (rid != null)
      throw new IllegalStateException("Cannot update a record in a custom bucket");

    final JSONObject json = record.toJSON();
    json.remove(RID_PROPERTY);  // Remove @rid to avoid SQL parsing issues
    final ResultSet result = command("sql",
        "insert into " + record.getTypeName() + " bucket " + bucketName + " content " + json);
    final RID newRID = result.next().getIdentity().get();
    trackCreatedRecord(record);
    return newRID;
  }

  protected Map<String, Object> mapArgs(final Object[] args) {
    Map<String, Object> params = null;
    if (args != null && args.length > 0) {
      if (args.length == 1 && args[0] instanceof Map)
        params = (Map<String, Object>) args[0];
      else {
        params = new HashMap<>();
        for (final Object o : args) {
          params.put("" + params.size(), o);
        }
      }
    }
    return params;
  }

  protected void checkDatabaseIsOpen() {
    if (!open)
      throw new DatabaseIsClosedException(databaseName);
  }

  public BinarySerializer getSerializer() {
    return serializer;
  }

  public void setSerializer(final BinarySerializer serializer) {
    this.serializer = serializer;
  }
}
