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
package com.arcadedb.database;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.database.async.DatabaseAsyncExecutor;
import com.arcadedb.database.async.ErrorCallback;
import com.arcadedb.database.async.NewEdgeCallback;
import com.arcadedb.database.async.OkCallback;
import com.arcadedb.engine.ErrorRecordCallback;
import com.arcadedb.engine.PaginatedFile;
import com.arcadedb.engine.WALFile;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;

import java.util.*;
import java.util.concurrent.*;

public interface Database extends AutoCloseable {
  ContextConfiguration getConfiguration();

  String getName();

  PaginatedFile.MODE getMode();

  @Override
  void close();

  boolean isOpen();

  void drop();

  DatabaseAsyncExecutor async();

  String getDatabasePath();

  /**
   * Return the current username if the database supports security. If used embedded, ArcadeDB does not provide a security model. If you want to use database
   * security, use ArcadeDB server.
   */
  String getCurrentUserName();

  /**
   * Returns true if there is a transaction active. A transaction is active if it is in the following states: `{BEGUN, COMMIT_1ST_PHASE, COMMIT_2ND_PHASE}`.
   */
  boolean isTransactionActive();

  /**
   * Executes a lambda in the transaction scope. If there is an active transaction, then the current transaction is temporarily parked and a new sub-transaction
   * is begun. In case an exception is thrown inside the lambda method, the transaction is rolled back.
   *
   * @param txBlock Transaction lambda to execute
   */
  void transaction(TransactionScope txBlock);

  /**
   * Executes a lambda in the transaction scope. If there is an active transaction, then the current transaction is temporarily parked and a new sub-transaction
   * is begun. In case an exception is thrown inside the lambda method, the transaction is rolled back. If joinCurrentTx is true, otherwise the current active
   * transaction is joined.
   *
   * @param txBlock       Transaction lambda to execute
   * @param joinCurrentTx if active joins the current transaction, otherwise always create a new one
   *
   * @return true if a new transaction has been created or false if an existent transaction has been joined
   */
  boolean transaction(TransactionScope txBlock, boolean joinCurrentTx);

  /**
   * Executes a lambda in the transaction scope. If there is an active transaction, then the current transaction is temporarily parked and a new sub-transaction
   * is begun.  In case an exception is thrown inside the lambda method, the transaction is rolled back. If joinCurrentTx is true, otherwise the current active
   * transaction is joined. The difference with the method {@link #transaction(TransactionScope)} is that in case the NeedRetryException exception is thrown, the
   * transaction is re-executed for a number of retries.
   *
   * @param txBlock       Transaction lambda to execute
   * @param joinCurrentTx if active joins the current transaction, otherwise always create a new one
   * @param retries       number of retries in case the NeedRetryException exception is thrown
   *
   * @return true if a new transaction has been created or false if an existent transaction has been joined
   */
  boolean transaction(TransactionScope txBlock, boolean joinCurrentTx, int retries);

  /**
   * Executes a lambda in the transaction scope. If there is an active transaction, then the current transaction is parked and a new sub-transaction is begun
   * if joinCurrentTx is true, otherwise the current active transaction is joined.
   * The difference with the method {@link #transaction(TransactionScope)} is that in case the NeedRetryException exception is thrown, the transaction is
   * re-executed for a number of retries.
   *
   * @param txBlock       Transaction lambda to execute
   * @param joinCurrentTx if active joins the current transaction, otherwise always create a new one
   * @param attempts      number of attempts in case the NeedRetryException exception is thrown
   * @param ok            callback invoked if the transaction completes the commit
   * @param error         callback invoked if the transaction cannot complete the commit, after the rollback
   *
   * @return true if a new transaction has been created or false if an existent transaction has been joined
   */
  boolean transaction(TransactionScope txBlock, boolean joinCurrentTx, int attempts, final OkCallback ok, final ErrorCallback error);

  /**
   * Returns true if a transaction is started automatically for all non-idempotent operation in the database.
   */
  boolean isAutoTransaction();

  void setAutoTransaction(boolean autoTransaction);

  /**
   * Begins a new transaction. If a transaction is already begun, the current transaction is parked and a new sub-transaction is begun. The new sub-transaction
   * does not access to the content of the previous transaction. Sub transactions are totally isolated.
   */
  void begin();

  /**
   * Commits the current transaction. If it was a sub-transaction, then the previous in the stack becomes active again.
   */
  void commit();

  /**
   * Rolls back the current transaction. If it was a sub-transaction, then the previous in the stack becomes active again.
   */
  void rollback();

  /**
   * Rolls back all the nested transactions if any.
   */
  void rollbackAllNested();

  /**
   * Scans the records contained in all the buckets defined by a type. This operation scans in sequence each bucket looking for documents, vertices and edges.
   * For each record found a call to #DocumentCallback.onRecord is invoked. If the callback returns false, the scan is terminated, otherwise it continues to
   * the next record.
   *
   * @param typeName    The name of the type
   * @param polymorphic true if the records of all the subtypes must be included, otherwise only the records strictly contained in the #typeName
   *                    will be scanned
   * @param callback    Callback to handle the loaded record document. Returns false to interrupt the scan operation, otherwise true to continue till the end
   */
  void scanType(String typeName, boolean polymorphic, DocumentCallback callback);

  /**
   * Scans the records contained in all the buckets defined by a type. This operation scans in sequence each bucket looking for documents, vertices and edges.
   * For each record found a call to #DocumentCallback.onRecord is invoked. If the callback returns false, the scan is terminated, otherwise it continues to
   * the next record.
   *
   * @param typeName            The name of the type
   * @param polymorphic         true if the records of all the subtypes must be included, otherwise only the records strictly contained in the #typeName
   *                            will be scanned
   * @param callback            Callback to handle the loaded record document. Returns false to interrupt the scan operation, otherwise true to continue till the end
   * @param errorRecordCallback Callback used in case of error during the scan
   */
  void scanType(String typeName, boolean polymorphic, DocumentCallback callback, ErrorRecordCallback errorRecordCallback);

  /**
   * Scans the records contained in the specified bucket. This operation scans in sequence each bucket.
   * For each record found a call to #RecordCallback.onRecord is invoked. If the callback returns false, the scan is terminated, otherwise it continues to
   * the next record.
   *
   * @param bucketName The name of the bucket
   * @param callback   Callback to handle the loaded record document. Returns false to interrupt the scan operation, otherwise true to continue till the end
   */
  void scanBucket(String bucketName, RecordCallback callback);

  /**
   * Scans the records contained in the specified bucket. This operation scans in sequence each bucket.
   * For each record found a call to #RecordCallback.onRecord is invoked. If the callback returns false, the scan is terminated, otherwise it continues to
   * the next record.
   *
   * @param bucketName          The name of the bucket
   * @param callback            Callback to handle the loaded record document. Returns false to interrupt the scan operation, otherwise true to continue till the end
   * @param errorRecordCallback Callback used in case of error during the scan
   */
  void scanBucket(String bucketName, RecordCallback callback, ErrorRecordCallback errorRecordCallback);

  /**
   * Lookups for a record by its @{@link RID} (record id). If #loadContent is true, the content is immediately loaded, otherwise the content will be loaded at
   * the first attempt to access to its content.
   *
   * @param rid         @{@link RID} record id
   * @param loadContent true to load the record content immediately, otherwise the content will be loaded at the first attempt to access to its content
   *
   * @return The record found
   *
   * @throws RecordNotFoundException If the record is not found
   */
  Record lookupByRID(RID rid, boolean loadContent);

  /**
   * Lookups for records of a specific type by a key and value. This operation requires an index to be created against the property used in the key.
   *
   * @param type     Type name
   * @param keyName  Name of the property defined in the index to use
   * @param keyValue Value to look for in the index
   *
   * @return A cursor containing the records found if any
   *
   * @throws IllegalArgumentException If an index is not defined on the `type.keyName` property
   */
  IndexCursor lookupByKey(String type, String keyName, Object keyValue);

  /**
   * Lookups for records of a specific type by a set of keys and values. This operation requires an index to be created against the properties used in the key.
   *
   * @param type      Type name
   * @param keyNames  Names of the property defined in the index to use
   * @param keyValues Values to look for in the index
   *
   * @return A cursor containing the records found if any
   *
   * @throws IllegalArgumentException If an index is not defined on the `type.keyNames` property
   */
  IndexCursor lookupByKey(String type, String[] keyNames, Object[] keyValues);

  /**
   * Iterates the records contained in all the buckets defined by a type. This operation iterates in sequence each bucket.
   *
   * @param typeName    The name of the type
   * @param polymorphic true if the records of all the subtypes must be included, otherwise only the records strictly contained in the #typeName
   *                    will be scanned
   *
   * @return The number of records found
   */
  Iterator<Record> iterateType(String typeName, boolean polymorphic);

  /**
   * Iterates the records contained in a bucket.
   *
   * @param bucketName The name of the bucket
   *
   * @return The number of records found
   */
  Iterator<Record> iterateBucket(String bucketName);

  /**
   * Returns the number of record contained in all the buckets defined by a type. This operation is expensive because it scans all the entire buckets.
   *
   * @param typeName    The name of the type
   * @param polymorphic true if the records of all the subtypes must be included, otherwise only the records strictly contained in the #typeName
   *                    will be scanned
   *
   * @return The number of records found
   */
  long countType(String typeName, boolean polymorphic);

  /**
   * Returns the number of record contained in a bucket. This operation is expensive because it scans the entire bucket.
   *
   * @param bucketName The name of the bucket
   *
   * @return The number of records found
   */
  long countBucket(String bucketName);

  /**
   * Creates a new document of type typeName. The type must be defined beforehand in the schema. The returned document only lives in memory until the method
   * {@link MutableDocument#save()} is executed.
   *
   * @param typeName Type name defined in the schema
   *
   * @return The new document as a MutableDocument object.
   */
  MutableDocument newDocument(String typeName);

  /**
   * Creates a new vertex of type typeName. The type must be defined beforehand in the schema. The returned vertex only lives in memory until the method
   * {@link MutableVertex#save()} is executed.
   *
   * @param typeName Type name defined in the schema
   *
   * @return The new vertex as a MutableVertex object.
   */
  MutableVertex newVertex(String typeName);

  /**
   * Creates a new edge between two vertices specifying the key/value pairs to lookup for both source and destination vertices. The direction of the edge is from source
   * to destination. This API is useful for bulk import of edges.
   *
   * @param sourceVertexType           Source vertex type name
   * @param sourceVertexKeyNames       Source vertex keys
   * @param sourceVertexKeyValues      Source vertex values
   * @param destinationVertexType      Destination vertex type name
   * @param destinationVertexKeyNames  Destination vertex keys
   * @param destinationVertexKeyValues Source vertex values
   * @param createVertexIfNotExist     True to create vertices if the lookup did not find the vertices. This is valid for both source and destination vertices.
   *                                   In case the vertices are implicitly created, only the properties specified in keys and values will be set
   * @param edgeType                   Type of the edge to create. The type must be defined in the schema beforehand
   * @param bidirectional              True to create a bidirectional edge. Using bidirectional edges is always the recommended setting, unless advanced
   *                                   fine-tuning on performances
   * @param properties                 Initial properties to set to the new edge as a variable argument array with key/value pairs
   *
   * @return The new edge. The edge is already persistent in the current transaction.
   *
   * @see DatabaseAsyncExecutor#newEdgeByKeys(String, String, Object, String, String, Object, boolean, String, boolean, boolean, NewEdgeCallback, Object...)
   * @see #newEdgeByKeys(Vertex, String, String[], Object[], boolean, String, boolean, Object...)
   */
  Edge newEdgeByKeys(String sourceVertexType, String[] sourceVertexKeyNames, Object[] sourceVertexKeyValues, String destinationVertexType,
      String[] destinationVertexKeyNames, Object[] destinationVertexKeyValues, boolean createVertexIfNotExist, String edgeType, boolean bidirectional,
      Object... properties);

  /**
   * Creates a new edge between two vertices specifying the source vertex instance and the key/value pairs to lookup for the destination vertices. The direction
   * of the edge is from source to destination. This API is useful for bulk import of edges.
   *
   * @param sourceVertex               Source vertex instance
   * @param destinationVertexType      Destination vertex type name
   * @param destinationVertexKeyNames  Destination vertex keys
   * @param destinationVertexKeyValues Source vertex values
   * @param createVertexIfNotExist     True to create the destination vertex if the lookup did not find the destination vertex.
   *                                   In case the destination vertex is implicitly created, only the properties specified in keys and values will be set
   * @param edgeType                   Type of the edge to create. The type must be defined in the schema beforehand
   * @param bidirectional              True to create a bidirectional edge. Using bidirectional edges is always the recommended setting, unless advanced
   *                                   fine-tuning on performances
   * @param properties                 Initial properties to set to the new edge as a variable argument array with key/value pairs
   *
   * @return The new edge. The edge is already persistent in the current transaction.
   *
   * @see DatabaseAsyncExecutor#newEdgeByKeys(String, String, Object, String, String, Object, boolean, String, boolean, boolean, NewEdgeCallback, Object...)
   * @see #newEdgeByKeys(String, String[], Object[], String, String[], Object[], boolean, String, boolean, Object...)
   */
  Edge newEdgeByKeys(Vertex sourceVertex, String destinationVertexType, String[] destinationVertexKeyNames, Object[] destinationVertexKeyValues,
      boolean createVertexIfNotExist, String edgeType, boolean bidirectional, Object... properties);

  /**
   * Deletes a record. The actual deletion will be effective only at transaction commit time. This operation is not allowed in databases open in read-only mode.
   *
   * @param record The record to delete
   */
  void deleteRecord(Record record);

  /**
   * Returns the database schema.
   */
  Schema getSchema();

  /**
   * Returns the record events listeners interface to manage listeners.
   */
  RecordEvents getEvents();

  /**
   * Executes a command by specifying the language and an optional variable array of arguments.
   *
   * @param language The language to use between the supported ones ("sql", "gremlin", "cypher", "graphql", "mongo", etc.)
   * @param query    The command to be interpreted in the specified language as a string
   * @param args     (optional) Arguments to pass to the command as a variable length array
   *
   * @return The {@link ResultSet} object containing the result of the operation if succeeded, otherwise a runtime exception is thrown
   */
  ResultSet command(String language, String query, Object... args);

  /**
   * Executes a command by specifying the language and arguments in a map.
   *
   * @param language The language to use between the supported ones ("sql", "gremlin", "cypher", "graphql", "mongo", etc.)
   * @param query    The command to be interpreted in the specified language as a string
   * @param args     Arguments to pass to the command as a map of name/values.
   *
   * @return The {@link ResultSet} object containing the result of the operation if succeeded, otherwise a runtime exception is thrown
   */
  ResultSet command(String language, String query, Map<String, Object> args);

  /**
   * Executes a query as an idempotent (read only) command by specifying the language and an optional variable array of arguments.
   *
   * @param language The language to use between the supported ones ("sql", "gremlin", "cypher", "graphql", "mongo", etc.)
   * @param query    The command to be interpreted in the specified language as a string
   * @param args     (optional) Arguments to pass to the command as a variable length array
   *
   * @return The {@link ResultSet} object containing the result of the operation if succeeded, otherwise a runtime exception is thrown
   */
  ResultSet query(String language, String query, Object... args);

  /**
   * Executes a query as an idempotent (read only) command by specifying the language and arguments in a map.
   *
   * @param language The language to use between the supported ones ("sql", "gremlin", "cypher", "graphql", "mongo", etc.)
   * @param query    The command to be interpreted in the specified language as a string
   * @param args     Arguments to pass to the command as a map of name/values.
   *
   * @return The {@link ResultSet} object containing the result of the operation if succeeded, otherwise a runtime exception is thrown
   */
  ResultSet query(String language, String query, Map<String, Object> args);

  ResultSet execute(String language, String script, Object... args);

  ResultSet execute(String language, String script, Map<String, Object> args);

  /**
   * Executes an operation in database read (shared) lock.
   */
  <RET extends Object> RET executeInReadLock(Callable<RET> callable);

  /**
   * Executes an operation in database write (exclusive) lock.
   */
  <RET extends Object> RET executeInWriteLock(Callable<RET> callable);

  /**
   * If enabled, writes the writes to the database are immediately available in queries and lookups.
   *
   * @return true if "ready your write" setting is enabled, otherwise false.
   */
  boolean isReadYourWrites();

  /**
   * Tells to the database writes must be immediately available for following reads and lookups. Disable this setting to speedup massive insertion workloads.
   *
   * @param value set to true to enable the "ready your write" setting, otherwise false.
   *
   * @return Current Database instance to execute setter methods in chain.
   */
  Database setReadYourWrites(boolean value);

  /**
   * Returns the current default edge list initial size to hold and store edges.
   */
  int getEdgeListSize();

  /**
   * Modifies the default edge list initial size to hold and store edges.
   *
   * @param size new size of the list
   *
   * @return Current Database instance to execute setter methods in chain.
   */
  Database setEdgeListSize(int size);

  /**
   * Changes the settings about using the WAL (Write Ahead Log - Transaction Journal) for transactions. By default, the WAL is enabled and preserve the database
   * in case of crash. Disabling the WAL is not recommended unless initial importing of the database or bulk loading.
   *
   * @param useWAL true to use the WAL, otherwise false
   *
   * @return Current Database instance to execute setter methods in chain.
   */
  Database setUseWAL(boolean useWAL);

  /**
   * Sets the WAL (Write Ahead Log - Transaction Journal) flush strategy.
   *
   * @param flush The new value contained in the enum: NO (no flush), YES_NOMETADATA (flush only data, no metadata), YES_FULL (full flush)
   *
   * @return Current Database instance to execute setter methods in chain.
   *
   * @see DatabaseAsyncExecutor#setTransactionSync(WALFile.FLUSH_TYPE)
   */
  Database setWALFlush(WALFile.FLUSH_TYPE flush);

  /**
   * Returns the asynchronous flush setting. If enabled, modified pages in transactions are flushed to disk by using an asynchronous thread. This is the default
   * behaviour. While the WAL (Write Ahead Log - Transaction Journal) is always written synchronously to avoid data loss in case of crash, the actual data pages
   * write can be deferred because the updated information to save is contained also in the WAL and will be restored in case of crash.
   *
   * @return true if asynchronous flush setting is enabled, otherwise false
   *
   * @see #setAsyncFlush(boolean)
   */
  boolean isAsyncFlush();

  /**
   * Changes the default asynchronous flush setting. If enabled, modified pages in transactions are flushed to disk by using an asynchronous thread. This is the default
   * behaviour. While the WAL (Write Ahead Log - Transaction Journal) is always written synchronously to avoid data loss in case of crash, the actual data pages
   * write can be deferred because the updated information to save is contained also in the WAL and will be restored in case of crash.
   *
   * @return Current Database instance to execute setter methods in chain.
   *
   * @see #isAsyncFlush()
   */
  Database setAsyncFlush(boolean value);

  /**
   * Interface for defining a transaction scope to be executed by the {@link #transaction(TransactionScope)} method.
   */
  interface TransactionScope {
    /**
     * Callback executed inside a transaction. All the changes executed in this method will be executed in a transaction scope where all the operations are
     * committed or all roll backed in case of an exception is thrown.
     */
    void execute();
  }
}
