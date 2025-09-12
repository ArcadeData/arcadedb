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
