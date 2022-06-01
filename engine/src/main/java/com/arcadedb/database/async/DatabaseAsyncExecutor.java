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
package com.arcadedb.database.async;

import com.arcadedb.database.Database;
import com.arcadedb.database.DocumentCallback;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.engine.ErrorRecordCallback;
import com.arcadedb.engine.WALFile;
import com.arcadedb.graph.Vertex;

import java.util.*;

/**
 * Asynchronous executor returned by {@link Database#async()}. Use this interface to execute operations against the database in asynchronous way and in parallel,
 * based on the configured executor threads. ArcadeDB will optimize the execution on the available cores.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 **/
public interface DatabaseAsyncExecutor {
  /**
   * Waits for the completion of all the pending tasks.
   */
  void waitCompletion();

  /**
   * Waits for the completion of all the pending tasks.
   *
   * @param timeout timeout in milliseconds
   *
   * @return true if returns before the timeout expires or any interruptions, otherwise false
   */
  boolean waitCompletion(long timeout);

  /**
   * Schedules the execution of a query as an idempotent (read only) command by specifying the language and an optional variable array of arguments.
   *
   * @param language The language to use between the supported ones ("sql", "gremlin", "cypher", "graphql", "mongo", etc.)
   * @param query    The command to be interpreted in the specified language as a string
   * @param callback Callback to manage the query result
   * @param args     (optional) Arguments to pass to the command as a variable length array
   */
  void query(String language, String query, AsyncResultsetCallback callback, Object... args);

  /**
   * Schedules the execution of a query as an idempotent (read only) command by specifying the language and arguments in a map.
   *
   * @param language The language to use between the supported ones ("sql", "gremlin", "cypher", "graphql", "mongo", etc.)
   * @param query    The command to be interpreted in the specified language as a string
   * @param callback Callback to manage the query result
   * @param args     Arguments to pass to the command as a map of name/values
   */
  void query(String language, String query, AsyncResultsetCallback callback, Map<String, Object> args);

  /**
   * Schedules the execution of a command by specifying the language and an optional variable array of arguments.
   *
   * @param language The language to use between the supported ones ("sql", "gremlin", "cypher", "graphql", "mongo", etc.)
   * @param query    The command to be interpreted in the specified language as a string
   * @param callback Callback to manage the command result
   * @param args     (optional) Arguments to pass to the command as a variable length array
   */
  void command(String language, String query, AsyncResultsetCallback callback, Object... args);

  /**
   * Schedules the execution of a command by specifying the language and arguments in a map.
   *
   * @param language The language to use between the supported ones ("sql", "gremlin", "cypher", "graphql", "mongo", etc.)
   * @param query    The command to be interpreted in the specified language as a string
   * @param callback Callback to manage the command result
   * @param args     Arguments to pass to the command as a map of name/values
   */
  void command(String language, String query, AsyncResultsetCallback callback, Map<String, Object> args);

  /**
   * Schedules the scan of the records contained in all the buckets defined by a type. This operation scans in sequence each bucket looking for documents, vertices and edges.
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
   * Schedules the scan of the records contained in all the buckets defined by a type. This operation scans in sequence each bucket looking for documents, vertices and edges.
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
   * Schedules the request to execute a lambda in the transaction scope. In case an exception is thrown inside the lambda method, the transaction is rolled back.
   *
   * @param txBlock Transaction lambda to execute
   */
  void transaction(Database.TransactionScope txBlock);

  /**
   * Schedules the request to execute a lambda in the transaction scope. In case an exception is thrown inside the lambda method, the transaction is rolled back.
   * The difference with the method {@link #transaction(Database.TransactionScope)} is that in case the NeedRetryException exception is thrown, the
   * * transaction is re-executed for a number of retries.
   *
   * @param txBlock Transaction lambda to execute
   * @param retries Number of retries in case the NeedRetryException exception is thrown
   */
  void transaction(Database.TransactionScope txBlock, int retries);

  /**
   * Schedules the request to execute a lambda in the transaction scope. In case an exception is thrown inside the lambda method, the transaction is rolled back.
   * The difference with the method {@link #transaction(Database.TransactionScope)} is that in case the NeedRetryException exception is thrown, the
   * * transaction is re-executed for a number of retries.
   *
   * @param txBlock Transaction lambda to execute
   * @param retries Number of retries in case the NeedRetryException exception is thrown
   * @param ok      Callback executed in case the transaction is committed
   * @param error   Callback executed in case of error after the transaction is rolled back
   */
  void transaction(Database.TransactionScope txBlock, int retries, OkCallback ok, ErrorCallback error);

  /**
   * Schedules the request to create a record. If the record is created successfully, the callback @{@link NewRecordCallback} is executed.
   *
   * @param record            Record to create
   * @param newRecordCallback Callback invoked after the record has been created
   */
  void createRecord(MutableDocument record, NewRecordCallback newRecordCallback);

  /**
   * Schedules the request to create a record. If the record is created successfully, the callback @{@link NewRecordCallback} is executed.
   *
   * @param record            Record to create
   * @param newRecordCallback Callback invoked after the record has been created
   * @param errorCallback     Callback invoked in case of error
   */
  void createRecord(MutableDocument record, NewRecordCallback newRecordCallback, final ErrorCallback errorCallback);

  /**
   * Schedules the request to create a record in a specific bucket. The bucket must be one defined in the schema to store the records of the current type.
   * If the record is created successfully, the callback @{@link NewRecordCallback} is executed.
   *
   * @param record            Record to create
   * @param newRecordCallback Callback invoked after the record has been created
   */
  void createRecord(Record record, String bucketName, NewRecordCallback newRecordCallback);

  /**
   * Schedules the request to create a record in a specific bucket. The bucket must be one defined in the schema to store the records of the current type.
   * If the record is created successfully, the callback @{@link NewRecordCallback} is executed.
   *
   * @param record            Record to create
   * @param newRecordCallback Callback invoked after the record has been created
   * @param errorCallback     Callback invoked in case of error
   */
  void createRecord(Record record, String bucketName, NewRecordCallback newRecordCallback, final ErrorCallback errorCallback);

  /**
   * Schedules the request to update a record. If the record is updated successfully, the callback @{@link UpdatedRecordCallback} is executed.
   *
   * @param record               Record to update
   * @param updateRecordCallback Callback invoked after the record has been updated
   */
  void updateRecord(MutableDocument record, UpdatedRecordCallback updateRecordCallback);

  /**
   * Schedules the request to update a record. If the record is updated successfully, the callback @{@link UpdatedRecordCallback} is executed.
   *
   * @param record               Record to update
   * @param updateRecordCallback Callback invoked after the record has been updated
   * @param errorCallback        Callback invoked in case of error
   */
  void updateRecord(MutableDocument record, UpdatedRecordCallback updateRecordCallback, final ErrorCallback errorCallback);

  /**
   * Schedules the request to delete a record. If the record is deleted successfully, the callback @{@link DeletedRecordCallback} is executed.
   *
   * @param record               Record to delete
   * @param deleteRecordCallback Callback invoked after the record has been deleted
   */
  void deleteRecord(Record record, DeletedRecordCallback deleteRecordCallback);

  /**
   * Schedules the request to delete a record. If the record is deleted successfully, the callback @{@link DeletedRecordCallback} is executed.
   *
   * @param record               Record to delete
   * @param deleteRecordCallback Callback invoked after the record has been deleted
   * @param errorCallback        Callback invoked in case of error
   */
  void deleteRecord(Record record, DeletedRecordCallback deleteRecordCallback, final ErrorCallback errorCallback);

  /**
   * Schedules the creation of an edge between two vertices. If the edge is created successfully, the callback @{@link NewEdgeCallback} is executed.
   *
   * @param sourceVertex         Source vertex instance
   * @param edgeType             Type of the edge to create. The type must be defined in the schema beforehand
   * @param destinationVertexRID Destination vertex id as @{@link RID}
   * @param bidirectional        True to create a bidirectional edge. Using bidirectional edges is always the recommended setting, unless advanced
   *                             fine-tuning on performances
   * @param light                True to create a light-weight edge. Using bidirectional edges is always the recommended setting, unless advanced
   *                             fine-tuning on performances
   * @param callback             Callback invoked when the edge is created
   * @param properties           Initial properties to set to the new edge as a variable argument array with key/value pairs
   */
  void newEdge(Vertex sourceVertex, String edgeType, RID destinationVertexRID, boolean bidirectional, boolean light, NewEdgeCallback callback,
      Object... properties);

  /**
   * Schedules the request to creates a new edge between two vertices specifying the key/value pairs to lookup for both source and destination vertices. The direction of the edge is from source
   * to destination. This API is useful for bulk import of edges.
   *
   * @param sourceVertexType          Source vertex type name
   * @param sourceVertexKeyName       Source vertex key
   * @param sourceVertexKeyValue      Source vertex value
   * @param destinationVertexType     Destination vertex type name
   * @param destinationVertexKeyName  Destination vertex key
   * @param destinationVertexKeyValue Source vertex value
   * @param createVertexIfNotExist    True to create vertices if the lookup did not find the vertices. This is valid for both source and destination vertices.
   *                                  In case the vertices are implicitly created, only the properties specified in keys and values will be set
   * @param edgeType                  Type of the edge to create. The type must be defined in the schema beforehand
   * @param bidirectional             True to create a bidirectional edge. Using bidirectional edges is always the recommended setting, unless advanced
   *                                  fine-tuning on performances
   * @param light                     True to create a light-weight edge. Using bidirectional edges is always the recommended setting, unless advanced
   *                                  fine-tuning on performances
   * @param callback                  Callback invoked when the edge is created
   * @param properties                Initial properties to set to the new edge as a variable argument array with key/value pairs
   *
   * @see DatabaseAsyncExecutor#newEdgeByKeys(String, String, Object, String, String, Object, boolean, String, boolean, boolean, NewEdgeCallback, Object...)
   * @see #newEdgeByKeys(String, String[], Object[], String, String[], Object[], boolean, String, boolean, boolean, NewEdgeCallback, Object...)
   */
  void newEdgeByKeys(String sourceVertexType, String sourceVertexKeyName, Object sourceVertexKeyValue, String destinationVertexType,
      String destinationVertexKeyName, Object destinationVertexKeyValue, boolean createVertexIfNotExist, String edgeType, boolean bidirectional, boolean light,
      NewEdgeCallback callback, Object... properties);

  /**
   * Schedules the request to creates a new edge between two vertices specifying the key/value pairs to lookup for both source and destination vertices. The direction of the edge is from source
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
   * @param light                      True to create a light-weight edge. Using bidirectional edges is always the recommended setting, unless advanced
   *                                   fine-tuning on performances
   * @param callback                   Callback invoked when the edge is created
   * @param properties                 Initial properties to set to the new edge as a variable argument array with key/value pairs
   *
   * @see DatabaseAsyncExecutor#newEdgeByKeys(String, String, Object, String, String, Object, boolean, String, boolean, boolean, NewEdgeCallback, Object...)
   * @see #newEdgeByKeys(String, String[], Object[], String, String[], Object[], boolean, String, boolean, boolean, NewEdgeCallback, Object...)
   */
  void newEdgeByKeys(String sourceVertexType, String[] sourceVertexKeyNames, Object[] sourceVertexKeyValues, String destinationVertexType,
      String[] destinationVertexKeyNames, Object[] destinationVertexKeyValues, boolean createVertexIfNotExist, String edgeType, boolean bidirectional,
      boolean light, NewEdgeCallback callback, Object... properties);

  /**
   * Forces the shutdown of the asynchronous threads.
   */
  void kill();

  /**
   * Returns the number of executor threads for asynchronous operations.
   *
   * @return the number of executor threads for asynchronous operations
   */
  int getParallelLevel();

  /**
   * Changes the number of executor threads for asynchronous operations. It is recommended to keep this number equals or lower than the actual cores available
   * on the server.
   *
   * @param parallelLevel Number of executor threads
   */
  void setParallelLevel(int parallelLevel);

  /**
   * Returns the current settings for backpressure in terms of percentage from 0 (disabled) to 100 of how much the queue is full to activate backpressure.
   * The default setting is 0 (disabled).
   *
   * @return A value from 0 to 100. 0 means backpressure disabled
   */
  int getBackPressure();

  /**
   * Sets when the threshold when the backpressure is used. The percentage is how much the asynchronous queue is full before the backpressure kicks in. When the
   * backpressure is active, the request to enqueue asynchronous operations is delayed exponentially with the amount of free slots in the queue. The default
   * setting is 0 (disabled).
   *
   * @param percentage Value from 0 (disabled) to 100 of how much the queue is full to activate backpressure
   */
  void setBackPressure(int percentage);

  /**
   * Gets the current batch size in terms of number of operations.
   *
   * @return The batch size in terms of number of operations
   *
   * @see #setCommitEvery(int)
   */
  int getCommitEvery();

  /**
   * Sets the amount of operations contained in a batch to be automatically committed when the batch is full. The default value is defined in
   * {@link com.arcadedb.GlobalConfiguration#ASYNC_TX_BATCH_SIZE}. If the size of the batch of operations is 0, then no automatic commit is performed.
   *
   * @param commitEvery Number of operations in a batch
   *
   * @see #getCommitEvery()
   */
  void setCommitEvery(int commitEvery);

  /**
   * Returns the current settings if the WAL (Write Ahead Log - Transaction Journal) is active for asynchronous operations.
   *
   * @return true if active, otherwise false
   */
  boolean isTransactionUseWAL();

  /**
   * Changes the setting on using the WAL (Write Ahead Log - Transaction Journal) for asynchronous operations. The default setting is true = WAL enabled.
   * Disabling the WAL is not recommended for online operations, but it can be used for the initial load/import of the database.
   *
   * @param transactionUseWAL true to enable WAL for asynchronous operations, otherwise false
   */
  void setTransactionUseWAL(boolean transactionUseWAL);

  /**
   * Sets the WAL (Write Ahead Log - Transaction Journal) flush strategy for asynchronous operations.
   *
   * @param transactionSync The new value contained in the enum: NO (no flush), YES_NOMETADATA (flush only data, no metadata), YES_FULL (full flush)
   *
   * @return Current Database instance to execute setter methods in chain.
   *
   * @see Database#setWALFlush(WALFile.FLUSH_TYPE)
   */
  void setTransactionSync(WALFile.FLUSH_TYPE transactionSync);

  /**
   * Defines a global callback to handle all the returns from asynchronous operations completed without errors.
   *
   * @param callback Callback invoked when an asynchronous operation succeeds
   */
  void onOk(OkCallback callback);

  /**
   * Defines a global callback to handle all the errors from asynchronous operations.
   *
   * @param callback Callback invoked when an asynchronous operation fails
   */
  void onError(ErrorCallback callback);

  void onOk();

  void onError(Throwable e);
}
