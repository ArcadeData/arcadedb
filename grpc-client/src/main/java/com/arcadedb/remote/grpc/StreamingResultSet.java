package com.arcadedb.remote.grpc;

import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.server.grpc.GrpcRecord;
import com.arcadedb.server.grpc.QueryResult;
import io.grpc.stub.BlockingClientCall;
import com.arcadedb.log.LogManager;
import java.util.logging.Level;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A ResultSet implementation that lazily fetches results from a gRPC stream.
 * Supports both Record results and projection/aggregation results.
 */
class StreamingResultSet implements ResultSet {

  private final BlockingClientCall<?, QueryResult> stream;
  private final RemoteGrpcDatabase                 db;
  private final AtomicLong                         totalProcessed  = new AtomicLong(0);
  protected     Iterator<Result>                   currentBatch    = Collections.emptyIterator();
  private       boolean                            streamExhausted = false;
  private       Result                             nextResult      = null;

  StreamingResultSet(BlockingClientCall<?, QueryResult> stream, RemoteGrpcDatabase db) {
    this.stream = stream;
    this.db = db;
  }

  @Override
  public boolean hasNext() {
    if (nextResult != null) {
      return true;
    }

    // Try to get next from current batch
    if (currentBatch.hasNext()) {
      nextResult = currentBatch.next();
      return true;
    }

    // Current batch exhausted, try to fetch next batch
    if (streamExhausted) {
      return false;
    }

    db.checkCrossThreadUse("streamQuery.hasNext");

    try {
      while (stream.hasNext()) {
        final QueryResult queryResult = stream.read();

        if (LogManager.instance().isDebugEnabled()) {
          LogManager.instance().log(this, Level.FINE, "Received batch with %d records, isLastBatch=%s", queryResult.getRecordsCount(),
              queryResult.getIsLastBatch());
        }

        if (queryResult.getRecordsCount() == 0) {
          if (queryResult.getIsLastBatch()) {
            streamExhausted = true;
            return false;
          }
          continue; // empty non-terminal batch
        }

        // Convert GrpcRecords to Results
        currentBatch = convertBatchToResults(queryResult);

        if (currentBatch.hasNext()) {
          nextResult = currentBatch.next();
          return true;
        }
      }

      streamExhausted = true;
      return false;

    } catch (io.grpc.StatusRuntimeException | io.grpc.StatusException e) {
      db.handleGrpcException(e);
      throw new IllegalStateException("unreachable");
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Stream interrupted", e);
    } catch (RuntimeException re) {
      throw re;
    } catch (Exception e) {
      throw new RuntimeException("Stream failed", e);
    }
  }

  /**
   * Convert a QueryResult batch to an Iterator of Result objects
   */
  protected Iterator<Result> convertBatchToResults(QueryResult queryResult) {
    List<Result> results = new ArrayList<>(queryResult.getRecordsCount());

    for (GrpcRecord grpcRecord : queryResult.getRecordsList()) {
      // Use the existing grpcRecordToResult method from RemoteGrpcDatabase
      Result result = db.grpcRecordToResult(grpcRecord);
      results.add(result);
    }

    return results.iterator();
  }

  @Override
  public Result next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }

    db.checkCrossThreadUse("streamQuery.next");

    Result result = nextResult;
    nextResult = null;
    totalProcessed.incrementAndGet();
    return result;
  }

  @Override
  public void close() {

    try {
      // Drain any remaining results
      while (stream.hasNext()) {
        stream.read();
      }
    } catch (Exception e) {
      LogManager.instance().log(this, Level.FINE, "Exception while draining stream during close: %s", e.getMessage());
    }

    // BlockingClientCall doesn't implement AutoCloseable
    // No need to cast or check instanceof
  }
}
