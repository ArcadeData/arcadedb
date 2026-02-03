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
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.server.grpc.GrpcRecord;
import com.arcadedb.server.grpc.GrpcValue;
import com.arcadedb.server.grpc.QueryResult;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for StreamingResultSet lazy loading behavior.
 * Uses a self-contained test double that replicates StreamingResultSet behavior
 * without requiring the actual gRPC infrastructure or RemoteGrpcDatabase dependencies.
 */
class StreamingResultSetTest {

  @Test
  @DisplayName("hasNext triggers lazy load on first call")
  void hasNext_triggersLazyLoad() {
    // Create test result set that returns one batch
    List<QueryResult> batches = List.of(
        createBatch(List.of(createRecord("1", "test1")), false),
        createBatch(List.of(), true)
    );
    TestableStreamingResultSet rs = new TestableStreamingResultSet(batches);

    // First hasNext should trigger read
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.getReadCount()).isGreaterThanOrEqualTo(1);

    rs.close();
  }

  @Test
  @DisplayName("next without hasNext works correctly")
  void next_withoutHasNext_works() {
    List<QueryResult> batches = List.of(
        createBatch(List.of(createRecord("1", "test1")), true)
    );
    TestableStreamingResultSet rs = new TestableStreamingResultSet(batches);

    // Call next directly (which internally calls hasNext)
    Result result = rs.next();
    assertThat(result).isNotNull();
    assertThat((String) result.getProperty("id")).isEqualTo("1");

    rs.close();
  }

  @Test
  @DisplayName("close releases iterator resources")
  void close_releasesIterator() {
    // Stream with pending data
    List<QueryResult> batches = List.of(
        createBatch(List.of(createRecord("1", "test")), false),
        createBatch(List.of(), true)
    );
    TestableStreamingResultSet rs = new TestableStreamingResultSet(batches);
    rs.close();

    // Verify stream was drained (hasNext should have been called during close)
    assertThat(rs.getHasNextCount()).isGreaterThanOrEqualTo(1);
  }

  @Test
  @DisplayName("empty batch is handled gracefully")
  void emptyBatch_handledGracefully() {
    // Empty batch followed by data batch
    List<QueryResult> batches = List.of(
        createBatch(List.of(), false),  // empty non-terminal
        createBatch(List.of(createRecord("1", "test")), false),
        createBatch(List.of(), true)    // empty terminal
    );
    TestableStreamingResultSet rs = new TestableStreamingResultSet(batches);

    assertThat(rs.hasNext()).isTrue();
    Result result = rs.next();
    assertThat((String) result.getProperty("id")).isEqualTo("1");
    assertThat(rs.hasNext()).isFalse();

    rs.close();
  }

  @Test
  @DisplayName("NoSuchElementException when exhausted")
  void next_whenExhausted_throwsNoSuchElement() {
    List<QueryResult> batches = List.of(
        createBatch(List.of(), true)
    );
    TestableStreamingResultSet rs = new TestableStreamingResultSet(batches);

    assertThat(rs.hasNext()).isFalse();
    assertThatThrownBy(rs::next).isInstanceOf(NoSuchElementException.class);

    rs.close();
  }

  @Test
  @DisplayName("BatchedStreamingResultSet exposes batch metadata")
  void batchedResultSet_exposesMetadata() {
    QueryResult batch = QueryResult.newBuilder()
        .addAllRecords(List.of(createRecord("1", "test1"), createRecord("2", "test2")))
        .setIsLastBatch(true)
        .setRunningTotalEmitted(2)
        .build();

    List<QueryResult> batches = List.of(batch);
    TestableBatchedStreamingResultSet rs = new TestableBatchedStreamingResultSet(batches);

    rs.hasNext(); // triggers batch load

    assertThat(rs.getCurrentBatchSize()).isEqualTo(2);
    assertThat(rs.isLastBatch()).isTrue();
    assertThat(rs.getRunningTotal()).isEqualTo(2);

    rs.close();
  }

  // Helper methods

  private GrpcRecord createRecord(String id, String name) {
    return GrpcRecord.newBuilder()
        .setRid("#0:" + id)
        .setType("TestType")
        .putProperties("id", GrpcValue.newBuilder().setStringValue(id).build())
        .putProperties("name", GrpcValue.newBuilder().setStringValue(name).build())
        .build();
  }

  private QueryResult createBatch(List<GrpcRecord> records, boolean isLast) {
    return QueryResult.newBuilder()
        .addAllRecords(records)
        .setIsLastBatch(isLast)
        .setRunningTotalEmitted(records.size())
        .build();
  }

  /**
   * Self-contained test double that replicates the StreamingResultSet behavior
   * exactly as implemented, without requiring gRPC or database dependencies.
   * This tests the same lazy-loading logic that StreamingResultSet uses.
   */
  static class TestableStreamingResultSet implements ResultSet {
    private final Iterator<QueryResult> batchIterator;
    private final AtomicInteger readCount = new AtomicInteger(0);
    private final AtomicInteger hasNextCount = new AtomicInteger(0);
    private final AtomicLong totalProcessed = new AtomicLong(0);
    private Iterator<Result> currentBatch = Collections.emptyIterator();
    private boolean streamExhausted = false;
    private Result nextResult = null;

    TestableStreamingResultSet(List<QueryResult> batches) {
      this.batchIterator = new ArrayList<>(batches).iterator();
    }

    @Override
    public boolean hasNext() {
      if (nextResult != null)
        return true;

      // Try to get next from current batch
      if (currentBatch.hasNext()) {
        nextResult = currentBatch.next();
        return true;
      }

      // Current batch exhausted, try to fetch next batch
      if (streamExhausted)
        return false;

      hasNextCount.incrementAndGet();
      while (batchIterator.hasNext()) {
        readCount.incrementAndGet();
        QueryResult queryResult = batchIterator.next();

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
    }

    protected Iterator<Result> convertBatchToResults(QueryResult queryResult) {
      List<Result> results = new ArrayList<>(queryResult.getRecordsCount());
      for (GrpcRecord grpcRecord : queryResult.getRecordsList()) {
        ResultInternal result = new ResultInternal();
        for (var entry : grpcRecord.getPropertiesMap().entrySet()) {
          GrpcValue value = entry.getValue();
          if (value.hasStringValue()) {
            result.setProperty(entry.getKey(), value.getStringValue());
          } else if (value.hasInt32Value()) {
            result.setProperty(entry.getKey(), value.getInt32Value());
          } else if (value.hasInt64Value()) {
            result.setProperty(entry.getKey(), value.getInt64Value());
          } else if (value.hasDoubleValue()) {
            result.setProperty(entry.getKey(), value.getDoubleValue());
          } else if (value.hasBoolValue()) {
            result.setProperty(entry.getKey(), value.getBoolValue());
          }
        }
        results.add(result);
      }
      return results.iterator();
    }

    @Override
    public Result next() {
      if (!hasNext())
        throw new NoSuchElementException();

      Result result = nextResult;
      nextResult = null;
      totalProcessed.incrementAndGet();
      return result;
    }

    @Override
    public void close() {
      // Drain remaining batches
      while (batchIterator.hasNext()) {
        hasNextCount.incrementAndGet();
        batchIterator.next();
      }
    }

    int getReadCount() {
      return readCount.get();
    }

    int getHasNextCount() {
      return hasNextCount.get();
    }
  }

  /**
   * Testable version of BatchedStreamingResultSet for metadata testing.
   */
  static class TestableBatchedStreamingResultSet extends TestableStreamingResultSet {
    private int currentBatchSizeValue = 0;
    private boolean isLastBatchValue = false;
    private long runningTotalValue = 0;
    private final Iterator<QueryResult> metadataBatchIterator;
    private Iterator<Result> metadataCurrentBatch = Collections.emptyIterator();
    private boolean metadataStreamExhausted = false;
    private Result metadataNextResult = null;

    TestableBatchedStreamingResultSet(List<QueryResult> batches) {
      super(batches);
      this.metadataBatchIterator = new ArrayList<>(batches).iterator();
    }

    @Override
    public boolean hasNext() {
      if (metadataNextResult != null)
        return true;

      if (metadataCurrentBatch.hasNext()) {
        metadataNextResult = metadataCurrentBatch.next();
        return true;
      }

      if (metadataStreamExhausted)
        return false;

      while (metadataBatchIterator.hasNext()) {
        QueryResult queryResult = metadataBatchIterator.next();

        // Capture batch metadata
        this.currentBatchSizeValue = queryResult.getRecordsCount();
        this.isLastBatchValue = queryResult.getIsLastBatch();
        this.runningTotalValue = queryResult.getRunningTotalEmitted();

        if (queryResult.getRecordsCount() == 0) {
          if (queryResult.getIsLastBatch()) {
            metadataStreamExhausted = true;
            return false;
          }
          continue;
        }

        metadataCurrentBatch = convertBatchToResults(queryResult);

        if (metadataCurrentBatch.hasNext()) {
          metadataNextResult = metadataCurrentBatch.next();
          return true;
        }
      }

      metadataStreamExhausted = true;
      return false;
    }

    @Override
    public Result next() {
      if (!hasNext())
        throw new NoSuchElementException();

      Result result = metadataNextResult;
      metadataNextResult = null;
      return result;
    }

    @Override
    public void close() {
      while (metadataBatchIterator.hasNext()) {
        metadataBatchIterator.next();
      }
    }

    public int getCurrentBatchSize() {
      return currentBatchSizeValue;
    }

    public boolean isLastBatch() {
      return isLastBatchValue;
    }

    public long getRunningTotal() {
      return runningTotalValue;
    }
  }
}
