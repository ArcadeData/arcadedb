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

import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.server.grpc.QueryResult;
import io.grpc.stub.BlockingClientCall;

import java.util.Iterator;

/**
 * A ResultSet that exposes batch boundaries for advanced use cases while
 * maintaining the standard ResultSet interface.
 */
class BatchedStreamingResultSet extends StreamingResultSet {

  private int     currentBatchSize = 0;
  private boolean isLastBatch      = false;
  private long    runningTotal     = 0;

  BatchedStreamingResultSet(BlockingClientCall<?, QueryResult> stream, RemoteGrpcDatabase db) {
    super(stream, db);
  }

  @Override
  protected Iterator<Result> convertBatchToResults(QueryResult queryResult) {
    // Capture batch metadata
    this.currentBatchSize = queryResult.getRecordsCount();
    this.isLastBatch = queryResult.getIsLastBatch();
    this.runningTotal = queryResult.getRunningTotalEmitted();

    // Call parent implementation
    return super.convertBatchToResults(queryResult);
  }

  // Additional methods for batch-aware processing
  public long getRunningTotal() {
    return runningTotal;
  }

  public int getCurrentBatchSize() {
    return currentBatchSize;
  }

  public boolean isLastBatch() {
    return isLastBatch;
  }
}
