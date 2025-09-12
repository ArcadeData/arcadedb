package com.arcadedb.remote.grpc;

import com.arcadedb.database.Record;
import com.arcadedb.query.sql.executor.Result;

import java.util.ArrayList;
import java.util.List;

public final class QueryBatch {
  private final List<Result> results; // Changed from List<Record>
  private final int          totalInBatch;
  private final long         runningTotal;
  private final boolean      lastBatch;

  public QueryBatch(List<Result> results, int totalInBatch, long runningTotal, boolean lastBatch) {
    this.results = results;
    this.totalInBatch = totalInBatch;
    this.runningTotal = runningTotal;
    this.lastBatch = lastBatch;
  }

  public List<Result> results() {
    return results;
  }

  // Backward compatibility: provide records() method that extracts Records from
  // Results
  @Deprecated
  public List<com.arcadedb.database.Record> records() {
    List<Record> records = new ArrayList<>(results.size());
    for (Result result : results) {
      if (result.isElement()) {
        result.getRecord().ifPresent(records::add);
      }
    }
    return records;
  }

  public int totalInBatch() {
    return totalInBatch;
  }

  public long runningTotal() {
    return runningTotal;
  }

  public boolean isLastBatch() {
    return lastBatch;
  }
}
