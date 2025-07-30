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
import com.arcadedb.database.async.ErrorCallback;
import com.arcadedb.database.async.OkCallback;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.utility.ExcludeFromJacocoGeneratedReport;

import java.util.*;

@ExcludeFromJacocoGeneratedReport
public interface BasicDatabase extends AutoCloseable {
  String getName();

  String getDatabasePath();

  boolean isOpen();

  Schema getSchema();

  @Override
  void close();

  void drop();

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
   * Returns true if there is a transaction active. A transaction is active if it is in the following states: `{BEGUN, COMMIT_1ST_PHASE, COMMIT_2ND_PHASE}`.
   */
  boolean isTransactionActive();

  /**
   * Returns the amount of nested transactions. 1 means no nested transactions.
   */
  int getNestedTransactions();

  TransactionExplicitLock acquireLock();

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
  boolean transaction(TransactionScope txBlock, boolean joinCurrentTx, int attempts, final OkCallback ok,
      final ErrorCallback error);

  /**
   * Begins a new transaction. If a transaction is already begun, the current transaction is parked and a new sub-transaction is begun. The new sub-transaction
   * does not access to the content of the previous transaction. Sub transactions are totally isolated.
   */
  void begin();

  /**
   * Begins a new transaction specifying the isolation level. If a transaction is already begun, the current transaction is parked and a new sub-transaction is
   * begun. The new sub-transaction does not access to the content of the previous transaction. Sub transactions are totally isolated.
   *
   * @param isolationLevel Isolation level between the following: READ_COMMITTED, REPEATABLE_READ, SERIALIZABLE
   */
  public void begin(Database.TransactionIsolationLevel isolationLevel);

  /**
   * Commits the current transaction. If it was a sub-transaction, then the previous in the stack becomes active again.
   */
  void commit();

  /**
   * Rolls back the current transaction. If it was a sub-transaction, then the previous in the stack becomes active again.
   */
  void rollback();

  /**
   * Looks up for a record by its @{@link RID} (record id). If #loadContent is true, the content is immediately loaded, otherwise the content will be loaded at
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
   * Checks if the record exists.
   *
   * @param rid – @RID record id
   *
   * @return true if the record exists, otherwise false
   */
  boolean existsRecord(RID rid);

  /**
   * Deletes a record. The actual deletion will be effective only at transaction commit time. This operation is not allowed in databases open in read-only mode.
   *
   * @param record The record to delete
   */
  void deleteRecord(Record record);

  /**
   * Iterates the records contained in all the buckets defined by a type. This operation iterates in sequence each bucket.
   *
   * @param typeName    The name of the type
   * @param polymorphic true if the records of all the subtypes must be included, otherwise only the records strictly contained in the #typeName
   *                    will be scanned
   *
   * @return The number of records found
   */
  <T extends Record> Iterator<T> iterateType(String typeName, boolean polymorphic);

  /**
   * Iterates the records contained in a bucket.
   *
   * @param bucketName The name of the bucket
   *
   * @return The number of records found
   */
  <T extends Record> Iterator<T> iterateBucket(String bucketName);

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
   * Executes a command by specifying the language and an optional variable array of arguments.
   *
   * @param language      The language to use between the supported ones ("sql", "gremlin", "cypher", "graphql", "mongo", etc.)
   * @param query         The command to be interpreted in the specified language as a string
   * @param configuration Configuration to use. When executed from a server, the server configuration is used. If null, an empty configuration will be used
   * @param args          Arguments to pass to the command as a map of name/values.
   *
   * @return The {@link ResultSet} object containing the result of the operation if succeeded, otherwise a runtime exception is thrown
   */
  ResultSet command(String language, String query, ContextConfiguration configuration, Object... args);

  /**
   * Executes a command by specifying the language and an optional variable array of arguments.
   *
   * @param language      The language to use between the supported ones ("sql", "gremlin", "cypher", "graphql", "mongo", etc.)
   * @param query         The command to be interpreted in the specified language as a string
   * @param configuration Configuration to use. When executed from a server, the server configuration is used. If null, an empty configuration will be used
   * @param args          Arguments to pass to the command as a map of name/values.
   *
   * @return The {@link ResultSet} object containing the result of the operation if succeeded, otherwise a runtime exception is thrown
   */
  ResultSet command(String language, String query, ContextConfiguration configuration, Map<String, Object> args);

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

  /**
   * @Deprecated. Execute a script. Use the `command()` instead.
   */
  @Deprecated
  ResultSet execute(String language, String script, Object... args);

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
   * Interface for defining a transaction scope to be executed by the {@link #transaction(TransactionScope)} method.
   */

  /**
   * Returns statistics for the current database instance as a map.
   */
  Map<String, Object> getStats();

  interface TransactionScope {
    /**
     * Callback executed inside a transaction. All the changes executed in this method will be executed in a transaction scope where all the operations are
     * committed or all roll backed in case of an exception is thrown.
     */
    void execute();
  }
}
