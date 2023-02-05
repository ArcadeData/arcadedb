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

import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.ResultSet;

public interface BasicDatabase extends AutoCloseable {
  String getName();

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
   * Deletes a record. The actual deletion will be effective only at transaction commit time. This operation is not allowed in databases open in read-only mode.
   *
   * @param record The record to delete
   */
  void deleteRecord(Record record);

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

  @Deprecated
  ResultSet execute(String language, String script, Object... args);

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
