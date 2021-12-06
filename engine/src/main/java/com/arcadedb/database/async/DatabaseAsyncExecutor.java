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
 */
package com.arcadedb.database.async;

import com.arcadedb.database.Record;
import com.arcadedb.database.*;
import com.arcadedb.engine.ErrorRecordCallback;
import com.arcadedb.engine.WALFile;
import com.arcadedb.graph.Vertex;

import java.util.Map;

/**
 * Asynchronous executor. Use this to execute operation in parallel. ArcadeDB will optimize the execution on the available cores.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 **/
public interface DatabaseAsyncExecutor {
  void setTransactionUseWAL(boolean transactionUseWAL);

  boolean isTransactionUseWAL();

  void setTransactionSync(WALFile.FLUSH_TYPE transactionSync);

  void onOk(OkCallback callback);

  void onError(ErrorCallback callback);

  void waitCompletion();

  /**
   * Waits for the completion of all the pending tasks.
   *
   * @param timeout timeout in milliseconds
   *
   * @return true if returns before the timeout expires or any interruptions, otherwise false
   */
  boolean waitCompletion(long timeout);

  void query(String language, String query, AsyncResultsetCallback callback, Object... parameters);

  void query(String language, String query, AsyncResultsetCallback callback, Map<String, Object> parameters);

  void command(String language, String query, AsyncResultsetCallback callback, Object... parameters);

  void command(String language, String query, AsyncResultsetCallback callback, Map<String, Object> parameters);

  void scanType(String typeName, boolean polymorphic, DocumentCallback callback);

  void scanType(String typeName, boolean polymorphic, DocumentCallback callback, ErrorRecordCallback errorRecordCallback);

  void transaction(Database.TransactionScope txBlock);

  void transaction(Database.TransactionScope txBlock, int retries);

  void transaction(Database.TransactionScope txBlock, int retries, OkCallback ok, ErrorCallback error);

  void createRecord(MutableDocument record, NewRecordCallback newRecordCallback);

  void createRecord(Record record, String bucketName, NewRecordCallback newRecordCallback);

  void updateRecord(MutableDocument record, UpdatedRecordCallback updateRecordCallback);

  void newEdge(Vertex sourceVertex, String edgeType, RID destinationVertexRID, boolean bidirectional, boolean light, NewEdgeCallback callback,
      Object... properties);

  void newEdgeByKeys(String sourceVertexType, String sourceVertexKeyName, Object sourceVertexKeyValue, String destinationVertexType,
      String destinationVertexKeyName, Object destinationVertexKeyValue, boolean createVertexIfNotExist, String edgeType, boolean bidirectional, boolean light,
      NewEdgeCallback callback, Object... properties);

  void newEdgeByKeys(String sourceVertexType, String[] sourceVertexKeyNames, Object[] sourceVertexKeyValues, String destinationVertexType,
      String[] destinationVertexKeyNames, Object[] destinationVertexKeyValues, boolean createVertexIfNotExist, String edgeType, boolean bidirectional,
      boolean light, NewEdgeCallback callback, Object... properties);

  void kill();

  int getParallelLevel();

  void setParallelLevel(int parallelLevel);

  int getBackPressure();

  void setBackPressure(int percentage);

  int getCommitEvery();

  void setCommitEvery(int commitEvery);

  void onOk();

  void onError(Throwable e);
}
