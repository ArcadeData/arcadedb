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
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.BinarySerializer;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
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
  public static final String ARCADEDB_SESSION_ID = "arcadedb-session-id";

  private final String                               databaseName;
  private       BinarySerializer                     serializer;
  private       String                               sessionId;
  private       Database.TRANSACTION_ISOLATION_LEVEL transactionIsolationLevel =
      Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED;
  private final RemoteSchema                         schema                    = new RemoteSchema(this);
  private       boolean                              open                      = true;
  private       RemoteTransactionExplicitLock        explicitLock;
  private       int                                  cachedHashCode            = 0;

  public RemoteDatabase(final String server, final int port, final String databaseName, final String userName,
                        final String userPassword) {
    this(server, port, databaseName, userName, userPassword, new ContextConfiguration());
  }

  public RemoteDatabase(final String server, final int port, final String databaseName, final String userName,
                        final String userPassword, final ContextConfiguration configuration) {
    super(server, port, userName, userPassword, configuration);
    this.databaseName = databaseName;
    try {
      this.serializer = new BinarySerializer(configuration);
    } catch (ClassNotFoundException e) {
      LogManager.instance().log(this, Level.SEVERE, "Error creating BinarySerializer", e);
    }
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
        // RETRY
        lastException = e;
        setSessionId(null);

        if (error != null)
          error.call(e);

      } catch (final Exception e) {
        setSessionId(null);

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

  @Override
  public void begin() {
    begin(transactionIsolationLevel);
  }

  @Override
  public void begin(final Database.TRANSACTION_ISOLATION_LEVEL isolationLevel) {
    checkDatabaseIsOpen();
    if (getSessionId() != null)
      throw new TransactionException("Transaction already begun");

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

      setSessionId(response.headers().firstValue(ARCADEDB_SESSION_ID).orElse(null));
    } catch (final Exception e) {
      throw new TransactionException("Error on transaction begin", e);
    }
  }

  public void commit() {
    checkDatabaseIsOpen();
    stats.writeTx.incrementAndGet();

    if (getSessionId() == null)
      throw new TransactionException("Transaction not begun");

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
    } catch (final DuplicatedKeyException | ConcurrentModificationException e) {
      throw e;
    } catch (final Exception e) {
      throw new TransactionException("Error on transaction commit", e);
    } finally {
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
    } catch (final Exception e) {
      throw new TransactionException("Error on transaction rollback", e);
    } finally {
      setSessionId(null);
    }
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

    return (ResultSet) databaseCommand("command", language, command, params, false,
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
    return (ResultSet) databaseCommand("command", language, command, params, false,
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

  public Database.TRANSACTION_ISOLATION_LEVEL getTransactionIsolationLevel() {
    return transactionIsolationLevel;
  }

  public void setTransactionIsolationLevel(final Database.TRANSACTION_ISOLATION_LEVEL transactionIsolationLevel) {
    this.transactionIsolationLevel = transactionIsolationLevel;
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

  String getSessionId() {
    return sessionId;
  }

  protected void setSessionId(String sessionId) {
    this.sessionId = sessionId;
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

  protected ResultSet createResultSet(final JSONObject response) {
    final ResultSet resultSet = new InternalResultSet();

    final JSONArray resultArray = response.getJSONArray("result");
    for (int i = 0; i < resultArray.length(); ++i) {
      final JSONObject result = resultArray.getJSONObject(i);
      ((InternalResultSet) resultSet).add(json2Result(result));
    }
    return resultSet;
  }

  protected Result json2Result(final JSONObject result) {
    final Record record = json2Record(result);
    if (record == null)
      return new ResultInternal(result.toMap());

    return new ResultInternal(record);
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
    if (rid != null)
      command("sql", "update " + rid + " content " + json);
    else {
      final ResultSet result = command("sql", "insert into " + record.getTypeName() + " content " + json);
      rid = result.next().getIdentity().get();
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
    return result.next().getIdentity().get();
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
