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

import com.arcadedb.engine.FileManager;
import com.arcadedb.engine.PageManager;
import com.arcadedb.engine.TransactionManager;
import com.arcadedb.engine.WALFileFactory;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.graph.GraphEngine;
import com.arcadedb.query.sql.parser.ExecutionPlanCache;
import com.arcadedb.query.sql.parser.StatementCache;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.serializer.BinarySerializer;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Internal API, do not use as an end user.
 */
public interface DatabaseInternal extends Database {
  enum CALLBACK_EVENT {
    TX_AFTER_WAL_WRITE, DB_NOT_CLOSED
  }

  default TransactionContext getTransaction() {
    final TransactionContext tx = getTransactionIfExists();
    if (tx == null)
      throw new TransactionException("Transaction not started on current thread");
    return tx;
  }

  TransactionContext getTransactionIfExists();

  MutableEmbeddedDocument newEmbeddedDocument(EmbeddedModifier modifier, String typeName);

  DatabaseInternal getEmbedded();

  DatabaseContext.DatabaseContextTL getContext();

  FileManager getFileManager();

  RecordFactory getRecordFactory();

  BinarySerializer getSerializer();

  PageManager getPageManager();

  DatabaseInternal getWrappedDatabaseInstance();

  Map<String, Object> getWrappers();

  void setWrapper(final String name, final Object instance);

  void checkPermissionsOnDatabase(SecurityDatabaseUser.DATABASE_ACCESS access);

  void checkPermissionsOnFile(int fileId, SecurityDatabaseUser.ACCESS access);

  boolean checkTransactionIsActive(boolean createTx);

  boolean isAsyncProcessing();

  long getResultSetLimit();

  long getReadTimeout();

  void registerCallback(CALLBACK_EVENT event, Callable<Void> callback);

  void unregisterCallback(CALLBACK_EVENT event, Callable<Void> callback);

  void executeCallbacks(CALLBACK_EVENT event) throws IOException;

  GraphEngine getGraphEngine();

  TransactionManager getTransactionManager();

  void createRecord(MutableDocument record);

  void createRecord(Record record, String bucketName);

  void createRecordNoLock(Record record, String bucketName, boolean discardRecordAfter);

  void updateRecord(Record record);

  void updateRecordNoLock(Record record, boolean discardRecordAfter);

  void deleteRecordNoLock(Record record);

  Record invokeAfterReadEvents(Record record);

  void kill();

  DocumentIndexer getIndexer();

  WALFileFactory getWALFileFactory();

  StatementCache getStatementCache();

  ExecutionPlanCache getExecutionPlanCache();

  int getNewEdgeListSize(int previousSize);

  <RET> RET recordFileChanges(final Callable<Object> callback);

  void saveConfiguration() throws IOException;

  Map<String, Object> alignToReplicas();

  /**
   * Executes an operation after having locked files.
   */
  <RET> RET executeLockingFiles(Collection<Integer> fileIds, Callable<RET> callable);
}
